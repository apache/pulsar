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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OffloadCallback;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class OffloadPrefixTest extends MockedBookKeeperTestCase {
    private static final Logger log = LoggerFactory.getLogger(OffloadPrefixTest.class);

    @Test
    public void testNullOffloader() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);

        int i = 0;
        for (; i < 25; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        assertEquals(ledger.getLedgersInfoAsList().size(), 3);

        Position p = ledger.getLastConfirmedEntry();

        for (; i < 45; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        assertEquals(ledger.getLedgersInfoAsList().size(), 5);
        assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete()).count(), 0);
        try {
            ledger.offloadPrefix(p);
            fail("Should have thrown an exception");
        } catch (ManagedLedgerException e) {
            assertEquals(e.getCause().getClass(), CompletionException.class);
        }
        assertEquals(ledger.getLedgersInfoAsList().size(), 5);
        assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete()).count(), 0);

        // add more entries to ensure we can update the ledger list
        for (; i < 55; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        assertEquals(ledger.getLedgersInfoAsList().size(), 6);
        assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete()).count(), 0);
    }

    @Test
    public void testOffload() throws Exception {
        MockLedgerOffloader offloader = new MockLedgerOffloader();
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);

        int i = 0;
        for (; i < 25; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        assertEquals(ledger.getLedgersInfoAsList().size(), 3);

        ledger.offloadPrefix(ledger.getLastConfirmedEntry());

        assertEquals(ledger.getLedgersInfoAsList().size(), 3);
        assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete())
                            .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                            offloader.offloadedLedgers());
    }

    @Test
    public void testPositionOutOfRange() throws Exception {
        MockLedgerOffloader offloader = new MockLedgerOffloader();
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);

        int i = 0;
        for (; i < 25; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        assertEquals(ledger.getLedgersInfoAsList().size(), 3);

        try {
            ledger.offloadPrefix(PositionImpl.earliest);
            fail("Should have thrown an exception");
        } catch (ManagedLedgerException.InvalidCursorPositionException e) {
            // expected
        }
        try {
            ledger.offloadPrefix(PositionImpl.latest);
            fail("Should have thrown an exception");
        } catch (ManagedLedgerException.InvalidCursorPositionException e) {
            // expected
        }

        assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete()).count(), 0);
        assertEquals(offloader.offloadedLedgers().size(), 0);
    }

    @Test
    public void testPositionOnEdgeOfLedger() throws Exception {
        MockLedgerOffloader offloader = new MockLedgerOffloader();
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);

        int i = 0;
        for (; i < 20; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        assertEquals(ledger.getLedgersInfoAsList().size(), 2);

        Position p = ledger.getLastConfirmedEntry(); // position at end of second ledger

        ledger.addEntry("entry-blah".getBytes());
        assertEquals(ledger.getLedgersInfoAsList().size(), 3);

        PositionImpl firstUnoffloaded = (PositionImpl)ledger.offloadPrefix(p);

        // only the first ledger should have been offloaded
        assertEquals(ledger.getLedgersInfoAsList().size(), 3);
        assertEquals(offloader.offloadedLedgers().size(), 1);
        assertTrue(offloader.offloadedLedgers().contains(ledger.getLedgersInfoAsList().get(0).getLedgerId()));
        assertTrue(ledger.getLedgersInfoAsList().get(0).getOffloadContext().getComplete());
        assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete()).count(), 1);
        assertEquals(firstUnoffloaded.getLedgerId(), ledger.getLedgersInfoAsList().get(1).getLedgerId());
        assertEquals(firstUnoffloaded.getEntryId(), 0);

        // offload again, with the position in the third ledger
        PositionImpl firstUnoffloaded2 = (PositionImpl)ledger.offloadPrefix(ledger.getLastConfirmedEntry());
        assertEquals(ledger.getLedgersInfoAsList().size(), 3);
        assertEquals(offloader.offloadedLedgers().size(), 2);
        assertTrue(offloader.offloadedLedgers().contains(ledger.getLedgersInfoAsList().get(0).getLedgerId()));
        assertTrue(offloader.offloadedLedgers().contains(ledger.getLedgersInfoAsList().get(1).getLedgerId()));
        assertTrue(ledger.getLedgersInfoAsList().get(0).getOffloadContext().getComplete());
        assertTrue(ledger.getLedgersInfoAsList().get(1).getOffloadContext().getComplete());
        assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete()).count(), 2);
        assertEquals(firstUnoffloaded2.getLedgerId(), ledger.getLedgersInfoAsList().get(2).getLedgerId());
    }

    @Test
    public void testPositionOnLastEmptyLedger() throws Exception {
        MockLedgerOffloader offloader = new MockLedgerOffloader();
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);

        for (int i = 0; i < 5; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }

        // Re-open to trigger the case of empty ledger
        ledger.close();
        ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);

        assertEquals(ledger.getLedgersInfoAsList().size(), 2);

        assertTrue(ledger.getLedgersInfoAsList().get(0).getSize() > 0);
        assertEquals(ledger.getLedgersInfoAsList().get(1).getSize(), 0);

        // position past the end of first ledger
        Position p = new PositionImpl(ledger.getLedgersInfoAsList().get(1).getLedgerId(), 0);

        PositionImpl firstUnoffloaded = (PositionImpl)ledger.offloadPrefix(p);

        // only the first ledger should have been offloaded
        assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        assertEquals(offloader.offloadedLedgers().size(), 1);
        assertTrue(offloader.offloadedLedgers().contains(ledger.getLedgersInfoAsList().get(0).getLedgerId()));
        assertTrue(ledger.getLedgersInfoAsList().get(0).getOffloadContext().getComplete());
        assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete()).count(), 1);
        assertEquals(firstUnoffloaded.getLedgerId(), ledger.getLedgersInfoAsList().get(1).getLedgerId());
        assertEquals(firstUnoffloaded.getEntryId(), 0);
    }

    @Test
    public void testTrimOccursDuringOffload() throws Exception {
        CountDownLatch offloadStarted = new CountDownLatch(1);
        CompletableFuture<Void> blocker = new CompletableFuture<>();
        MockLedgerOffloader offloader = new MockLedgerOffloader() {
                @Override
                public CompletableFuture<Void> offload(ReadHandle ledger,
                                                       UUID uuid,
                                                       Map<String, String> extraMetadata) {
                    offloadStarted.countDown();
                    return blocker.thenCompose((f) -> super.offload(ledger, uuid, extraMetadata));
                }
            };

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(0, TimeUnit.MINUTES);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);
        ManagedCursor cursor = ledger.openCursor("foobar");

        // Create 3 ledgers, saving position at start of each
        for (int i = 0; i < 21; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        assertEquals(ledger.getLedgersInfoAsList().size(), 3);

        PositionImpl startOfSecondLedger = PositionImpl.get(ledger.getLedgersInfoAsList().get(1).getLedgerId(), 0);
        PositionImpl startOfThirdLedger = PositionImpl.get(ledger.getLedgersInfoAsList().get(2).getLedgerId(), 0);

        // trigger an offload which should offload the first two ledgers
        OffloadCallbackPromise cbPromise = new OffloadCallbackPromise();
        ledger.asyncOffloadPrefix(startOfThirdLedger, cbPromise, null);
        offloadStarted.await();

        // trim first ledger
        cursor.markDelete(startOfSecondLedger, new HashMap<>());
        assertEventuallyTrue(() -> ledger.getLedgersInfoAsList().size() == 2);
        assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete()).count(), 0);

        // complete offloading
        blocker.complete(null);
        cbPromise.get();

        assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete()).count(), 1);
        assertTrue(ledger.getLedgersInfoAsList().get(0).getOffloadContext().getComplete());
        assertEquals(offloader.offloadedLedgers().size(), 1);
        assertTrue(offloader.offloadedLedgers().contains(ledger.getLedgersInfoAsList().get(0).getLedgerId()));
    }

    @Test
    public void testTrimOccursDuringOffloadLedgerDeletedBeforeOffload() throws Exception {
        CountDownLatch offloadStarted = new CountDownLatch(1);
        CompletableFuture<Long> blocker = new CompletableFuture<>();
        MockLedgerOffloader offloader = new MockLedgerOffloader() {
                @Override
                public CompletableFuture<Void> offload(ReadHandle ledger,
                                                       UUID uuid,
                                                       Map<String, String> extraMetadata) {
                    offloadStarted.countDown();
                    return blocker.thenCompose(
                            (trimmedLedger) -> {
                                if (trimmedLedger == ledger.getId()) {
                                    CompletableFuture<Void> future = new CompletableFuture<>();
                                    future.completeExceptionally(new BKException.BKNoSuchLedgerExistsException());
                                    return future;
                                } else {
                                    return super.offload(ledger, uuid, extraMetadata);
                                }
                            });
                }
            };

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(0, TimeUnit.MINUTES);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);
        ManagedCursor cursor = ledger.openCursor("foobar");

        for (int i = 0; i < 21; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        assertEquals(ledger.getLedgersInfoAsList().size(), 3);

        PositionImpl startOfSecondLedger = PositionImpl.get(ledger.getLedgersInfoAsList().get(1).getLedgerId(), 0);
        PositionImpl startOfThirdLedger = PositionImpl.get(ledger.getLedgersInfoAsList().get(2).getLedgerId(), 0);

        // trigger an offload which should offload the first two ledgers
        OffloadCallbackPromise cbPromise = new OffloadCallbackPromise();
        ledger.asyncOffloadPrefix(startOfThirdLedger, cbPromise, null);
        offloadStarted.await();

        // trim first ledger
        long trimmedLedger = ledger.getLedgersInfoAsList().get(0).getLedgerId();
        cursor.markDelete(startOfSecondLedger, new HashMap<>());
        assertEventuallyTrue(() -> ledger.getLedgersInfoAsList().size() == 2);
        assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getLedgerId() == trimmedLedger).count(), 0);
        assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete()).count(), 0);

        // complete offloading
        blocker.complete(trimmedLedger);
        cbPromise.get();

        assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete()).count(), 1);
        assertTrue(ledger.getLedgersInfoAsList().get(0).getOffloadContext().getComplete());
        assertEquals(offloader.offloadedLedgers().size(), 1);
        assertTrue(offloader.offloadedLedgers().contains(ledger.getLedgersInfoAsList().get(0).getLedgerId()));
    }

    @Test
    public void testOffloadClosedManagedLedger() throws Exception {
        MockLedgerOffloader offloader = new MockLedgerOffloader();
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);

        for (int i = 0; i < 21; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }

        Position p = ledger.getLastConfirmedEntry();
        ledger.close();

        try {
            ledger.offloadPrefix(p);
            fail("Should fail because ML is closed");
        } catch (ManagedLedgerException.ManagedLedgerAlreadyClosedException e) {
            // expected
        }

        assertEquals(ledger.getLedgersInfoAsList().size(), 3);
        assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete()).count(), 0);
        assertEquals(offloader.offloadedLedgers().size(), 0);
    }

    @Test
    public void testOffloadSamePositionTwice() throws Exception {
        MockLedgerOffloader offloader = new MockLedgerOffloader();
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);

        int i = 0;
        for (; i < 25; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        assertEquals(ledger.getLedgersInfoAsList().size(), 3);

        ledger.offloadPrefix(ledger.getLastConfirmedEntry());

        assertEquals(ledger.getLedgersInfoAsList().size(), 3);
        assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete())
                            .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                            offloader.offloadedLedgers());

        ledger.offloadPrefix(ledger.getLastConfirmedEntry());

        assertEquals(ledger.getLedgersInfoAsList().size(), 3);
        assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete())
                            .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                            offloader.offloadedLedgers());

    }

    public void offloadThreeOneFails(int failIndex) throws Exception {
        CompletableFuture<Set<Long>> promise = new CompletableFuture<>();
        MockLedgerOffloader offloader = new ErroringMockLedgerOffloader(promise);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);

        int i = 0;
        for (; i < 35; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        assertEquals(ledger.getLedgersInfoAsList().size(), 4);

        // mark ledgers to fail
        promise.complete(ImmutableSet.of(ledger.getLedgersInfoAsList().get(failIndex).getLedgerId()));

        try {
            ledger.offloadPrefix(ledger.getLastConfirmedEntry());
        } catch (ManagedLedgerException e) {
            assertEquals(e.getCause().getClass(), CompletionException.class);
        }

        assertEquals(ledger.getLedgersInfoAsList().size(), 4);
        assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete())
                            .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                            offloader.offloadedLedgers());
        assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete()).count(), 2);
        assertFalse(ledger.getLedgersInfoAsList().get(failIndex).getOffloadContext().getComplete());
    }

    @Test
    public void testOffloadThreeFirstFails() throws Exception {
        offloadThreeOneFails(0);
    }

    @Test
    public void testOffloadThreeSecondFails() throws Exception {
        offloadThreeOneFails(1);
    }

    @Test
    public void testOffloadThreeThirdFails() throws Exception {
        offloadThreeOneFails(2);
    }

    @Test
    public void testOffloadNewML() throws Exception {
        MockLedgerOffloader offloader = new MockLedgerOffloader();
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);

        try {
            ledger.offloadPrefix(ledger.getLastConfirmedEntry());
        } catch (ManagedLedgerException.InvalidCursorPositionException e) {
            // expected
        }
        // add one entry and try again
        ledger.addEntry("foobar".getBytes());

        Position p = ledger.getLastConfirmedEntry();
        assertEquals(p, ledger.offloadPrefix(ledger.getLastConfirmedEntry()));
        assertEquals(ledger.getLedgersInfoAsList().size(), 1);
        assertEquals(offloader.offloadedLedgers().size(), 0);
    }

    @Test
    public void testOffloadConflict() throws Exception {
        Set<Pair<Long, UUID>> deleted = ConcurrentHashMap.newKeySet();
        CompletableFuture<Set<Long>> errorLedgers = new CompletableFuture<>();
        Set<Pair<Long, UUID>> failedOffloads = ConcurrentHashMap.newKeySet();

        MockLedgerOffloader offloader = new MockLedgerOffloader() {
                @Override
                public CompletableFuture<Void> offload(ReadHandle ledger,
                                                       UUID uuid,
                                                       Map<String, String> extraMetadata) {
                    return errorLedgers.thenCompose(
                            (errors) -> {
                                if (errors.remove(ledger.getId())) {
                                    failedOffloads.add(Pair.of(ledger.getId(), uuid));
                                    CompletableFuture<Void> future = new CompletableFuture<>();
                                    future.completeExceptionally(new Exception("Some kind of error"));
                                    return future;
                                } else {
                                    return super.offload(ledger, uuid, extraMetadata);
                                }
                            });
                }

                @Override
                public CompletableFuture<Void> deleteOffloaded(long ledgerId, UUID uuid,
                                                               Map<String, String> offloadDriverMetadata) {
                    deleted.add(Pair.of(ledgerId, uuid));
                    return super.deleteOffloaded(ledgerId, uuid, offloadDriverMetadata);
                }
            };
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);

        for (int i = 0; i < 15; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }

        Set<Long> errorSet = ConcurrentHashMap.newKeySet();
        errorSet.add(ledger.getLedgersInfoAsList().get(0).getLedgerId());
        errorLedgers.complete(errorSet);

        try {
            ledger.offloadPrefix(ledger.getLastConfirmedEntry());
        } catch (ManagedLedgerException e) {
            // expected
        }
        assertTrue(errorSet.isEmpty());
        assertEquals(failedOffloads.size(), 1);
        assertEquals(deleted.size(), 0);

        long expectedFailedLedger = ledger.getLedgersInfoAsList().get(0).getLedgerId();
        UUID expectedFailedUUID = new UUID(ledger.getLedgersInfoAsList().get(0).getOffloadContext().getUidMsb(),
                                           ledger.getLedgersInfoAsList().get(0).getOffloadContext().getUidLsb());
        assertEquals(failedOffloads.stream().findFirst().get(),
                            Pair.of(expectedFailedLedger, expectedFailedUUID));
        assertFalse(ledger.getLedgersInfoAsList().get(0).getOffloadContext().getComplete());

        // try offload again
        ledger.offloadPrefix(ledger.getLastConfirmedEntry());

        assertEquals(failedOffloads.size(), 1);
        assertEquals(deleted.size(), 1);
        assertEquals(deleted.stream().findFirst().get(),
                            Pair.of(expectedFailedLedger, expectedFailedUUID));
        UUID successUUID = new UUID(ledger.getLedgersInfoAsList().get(0).getOffloadContext().getUidMsb(),
                                    ledger.getLedgersInfoAsList().get(0).getOffloadContext().getUidLsb());
        assertNotEquals(expectedFailedUUID, successUUID);
        assertTrue(ledger.getLedgersInfoAsList().get(0).getOffloadContext().getComplete());
    }

    @Test
    public void testOffloadDelete() throws Exception {
        Set<Pair<Long, UUID>> deleted = ConcurrentHashMap.newKeySet();
        CompletableFuture<Set<Long>> errorLedgers = new CompletableFuture<>();
        Set<Pair<Long, UUID>> failedOffloads = ConcurrentHashMap.newKeySet();

        MockLedgerOffloader offloader = new MockLedgerOffloader();
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(0, TimeUnit.MINUTES);
        offloader.getOffloadPolicies().setManagedLedgerOffloadDeletionLagInMillis(100L);
        offloader.getOffloadPolicies().setManagedLedgerOffloadThresholdInBytes(100L);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);
        ManagedCursor cursor = ledger.openCursor("foobar");

        for (int i = 0; i < 15; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }

        assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        ledger.offloadPrefix(ledger.getLastConfirmedEntry());
        assertEquals(ledger.getLedgersInfoAsList().size(), 2);

        assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete()).count(), 1);
        assertTrue(ledger.getLedgersInfoAsList().get(0).getOffloadContext().getComplete());
        long firstLedger = ledger.getLedgersInfoAsList().get(0).getLedgerId();
        long secondLedger = ledger.getLedgersInfoAsList().get(1).getLedgerId();

        cursor.markDelete(ledger.getLastConfirmedEntry());
        assertEventuallyTrue(() -> ledger.getLedgersInfoAsList().size() == 1);
        assertEquals(ledger.getLedgersInfoAsList().get(0).getLedgerId(), secondLedger);

        assertEventuallyTrue(() -> offloader.deletedOffloads().contains(firstLedger));
    }

    @Test
    public void testOffloadDeleteIncomplete() throws Exception {
        Set<Pair<Long, UUID>> deleted = ConcurrentHashMap.newKeySet();
        CompletableFuture<Set<Long>> errorLedgers = new CompletableFuture<>();
        Set<Pair<Long, UUID>> failedOffloads = ConcurrentHashMap.newKeySet();

        MockLedgerOffloader offloader = new MockLedgerOffloader() {
                @Override
                public CompletableFuture<Void> offload(ReadHandle ledger,
                                                       UUID uuid,
                                                       Map<String, String> extraMetadata) {
                    return super.offload(ledger, uuid, extraMetadata)
                        .thenCompose((res) -> {
                                CompletableFuture<Void> f = new CompletableFuture<>();
                                f.completeExceptionally(new Exception("Fail after offload occurred"));
                                return f;
                            });
                }
            };
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(0, TimeUnit.MINUTES);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);
        ManagedCursor cursor = ledger.openCursor("foobar");
        for (int i = 0; i < 15; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }

        assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        try {
            ledger.offloadPrefix(ledger.getLastConfirmedEntry());
        } catch (ManagedLedgerException mle) {
            // expected
        }

        assertEquals(ledger.getLedgersInfoAsList().size(), 2);

        assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete()).count(), 0);
        assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().hasUidMsb()).count(), 1);
        assertTrue(ledger.getLedgersInfoAsList().get(0).getOffloadContext().hasUidMsb());

        long firstLedger = ledger.getLedgersInfoAsList().get(0).getLedgerId();
        long secondLedger = ledger.getLedgersInfoAsList().get(1).getLedgerId();

        cursor.markDelete(ledger.getLastConfirmedEntry());
        assertEventuallyTrue(() -> ledger.getLedgersInfoAsList().size() == 1);
        assertEquals(ledger.getLedgersInfoAsList().get(0).getLedgerId(), secondLedger);

        assertEventuallyTrue(() -> offloader.deletedOffloads().contains(firstLedger));
    }

    @Test
    public void testDontOffloadEmpty() throws Exception {
        MockLedgerOffloader offloader = new MockLedgerOffloader();
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);

        int i = 0;
        for (; i < 35; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        assertEquals(ledger.getLedgersInfoAsList().size(), 4);

        long firstLedgerId = ledger.getLedgersInfoAsList().get(0).getLedgerId();
        long secondLedgerId = ledger.getLedgersInfoAsList().get(1).getLedgerId();
        long thirdLedgerId = ledger.getLedgersInfoAsList().get(2).getLedgerId();
        long fourthLedgerId = ledger.getLedgersInfoAsList().get(3).getLedgerId();

        // make an ledger empty
        Field ledgersField = ledger.getClass().getDeclaredField("ledgers");
        ledgersField.setAccessible(true);
        Map<Long, LedgerInfo> ledgers = (Map<Long,LedgerInfo>)ledgersField.get(ledger);
        ledgers.put(secondLedgerId,
                    ledgers.get(secondLedgerId).toBuilder().setEntries(0).setSize(0).build());

        PositionImpl firstUnoffloaded = (PositionImpl)ledger.offloadPrefix(ledger.getLastConfirmedEntry());
        assertEquals(firstUnoffloaded.getLedgerId(), fourthLedgerId);
        assertEquals(firstUnoffloaded.getEntryId(), 0);

        assertEquals(ledger.getLedgersInfoAsList().size(), 4);
        assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete())
                            .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                            offloader.offloadedLedgers());
        assertEquals(offloader.offloadedLedgers(), ImmutableSet.of(firstLedgerId, thirdLedgerId));
    }

    private static byte[] buildEntry(int size, String pattern) {
        byte[] entry = new byte[size];
        byte[] patternBytes = pattern.getBytes();

        for (int i = 0; i < entry.length; i++) {
            entry[i] = patternBytes[i % patternBytes.length];
        }
        return entry;
    }

    @Test
    public void testAutoTriggerOffload() throws Exception {
        MockLedgerOffloader offloader = new MockLedgerOffloader();
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        offloader.getOffloadPolicies().setManagedLedgerOffloadThresholdInBytes(100L);
        config.setLedgerOffloader(offloader);

        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);

        // Ledger will roll twice, offload will run on first ledger after second closed
        for (int i = 0; i < 25; i++) {
            ledger.addEntry(buildEntry(10, "entry-" + i));
        }

        assertEquals(ledger.getLedgersInfoAsList().size(), 3);

        // offload should eventually be triggered
        assertEventuallyTrue(() -> offloader.offloadedLedgers().size() == 1);
        assertEquals(offloader.offloadedLedgers(),
                            ImmutableSet.of(ledger.getLedgersInfoAsList().get(0).getLedgerId()));
    }

    @Test
    public void manualTriggerWhileAutoInProgress() throws Exception {
        CompletableFuture<Void> slowOffload = new CompletableFuture<>();
        CountDownLatch offloadRunning = new CountDownLatch(1);
        MockLedgerOffloader offloader = new MockLedgerOffloader() {
                @Override
                public CompletableFuture<Void> offload(ReadHandle ledger,
                                                       UUID uuid,
                                                       Map<String, String> extraMetadata) {
                    offloadRunning.countDown();
                    return slowOffload.thenCompose((res) -> super.offload(ledger, uuid, extraMetadata));
                }
            };

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        offloader.getOffloadPolicies().setManagedLedgerOffloadThresholdInBytes(100L);
        config.setLedgerOffloader(offloader);

        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);

        // Ledger will roll twice, offload will run on first ledger after second closed
        for (int i = 0; i < 25; i++) {
            ledger.addEntry(buildEntry(10, "entry-" + i));
        }
        offloadRunning.await();

        for (int i = 0; i < 20; i++) {
            ledger.addEntry(buildEntry(10, "entry-" + i));
        }
        Position p = ledger.addEntry(buildEntry(10, "last-entry"));

        try {
            ledger.offloadPrefix(p);
            fail("Shouldn't have succeeded");
        } catch (ManagedLedgerException.OffloadInProgressException e) {
            // expected
        }

        slowOffload.complete(null);

        // eventually all over threshold will be offloaded
        assertEventuallyTrue(() -> offloader.offloadedLedgers().size() == 3);
        assertEquals(offloader.offloadedLedgers(),
                            ImmutableSet.of(ledger.getLedgersInfoAsList().get(0).getLedgerId(),
                                            ledger.getLedgersInfoAsList().get(1).getLedgerId(),
                                            ledger.getLedgersInfoAsList().get(2).getLedgerId()));

        // then a manual offload can run and offload the one ledger under the threshold
        ledger.offloadPrefix(p);

        assertEquals(offloader.offloadedLedgers().size(), 4);
        assertEquals(offloader.offloadedLedgers(),
                            ImmutableSet.of(ledger.getLedgersInfoAsList().get(0).getLedgerId(),
                                            ledger.getLedgersInfoAsList().get(1).getLedgerId(),
                                            ledger.getLedgersInfoAsList().get(2).getLedgerId(),
                                            ledger.getLedgersInfoAsList().get(3).getLedgerId()));
    }

    @Test
    public void autoTriggerWhileManualInProgress() throws Exception {
        CompletableFuture<Void> slowOffload = new CompletableFuture<>();
        CountDownLatch offloadRunning = new CountDownLatch(1);
        MockLedgerOffloader offloader = new MockLedgerOffloader() {
                @Override
                public CompletableFuture<Void> offload(ReadHandle ledger,
                                                       UUID uuid,
                                                       Map<String, String> extraMetadata) {
                    offloadRunning.countDown();
                    return slowOffload.thenCompose((res) -> super.offload(ledger, uuid, extraMetadata));
                }
            };

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        offloader.getOffloadPolicies().setManagedLedgerOffloadThresholdInBytes(100L);
        config.setLedgerOffloader(offloader);

        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);

        // Ledger rolls once, threshold not hit so auto shouldn't run
        for (int i = 0; i < 14; i++) {
            ledger.addEntry(buildEntry(10, "entry-" + i));
        }
        Position p = ledger.addEntry(buildEntry(10, "trigger-entry"));

        OffloadCallbackPromise cbPromise = new OffloadCallbackPromise();
        ledger.asyncOffloadPrefix(p, cbPromise, null);
        offloadRunning.await();

        // add enough entries to roll the ledger a couple of times and trigger some offloads
        for (int i = 0; i < 20; i++) {
            ledger.addEntry(buildEntry(10, "entry-" + i));
        }

        // allow the manual offload to complete
        slowOffload.complete(null);

        assertEquals(cbPromise.join(),
                            PositionImpl.get(ledger.getLedgersInfoAsList().get(1).getLedgerId(), 0));

        // auto trigger should eventually offload everything else over threshold
        assertEventuallyTrue(() -> offloader.offloadedLedgers().size() == 2);
        assertEquals(offloader.offloadedLedgers(),
                            ImmutableSet.of(ledger.getLedgersInfoAsList().get(0).getLedgerId(),
                                            ledger.getLedgersInfoAsList().get(1).getLedgerId()));
    }

    @Test
    public void multipleAutoTriggers() throws Exception {
        CompletableFuture<Void> slowOffload = new CompletableFuture<>();
        CountDownLatch offloadRunning = new CountDownLatch(1);
        MockLedgerOffloader offloader = new MockLedgerOffloader() {
                @Override
                public CompletableFuture<Void> offload(ReadHandle ledger,
                                                       UUID uuid,
                                                       Map<String, String> extraMetadata) {
                    offloadRunning.countDown();
                    return slowOffload.thenCompose((res) -> super.offload(ledger, uuid, extraMetadata));
                }
            };

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        offloader.getOffloadPolicies().setManagedLedgerOffloadThresholdInBytes(100L);
        config.setLedgerOffloader(offloader);

        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);

        // Ledger will roll twice, offload will run on first ledger after second closed
        for (int i = 0; i < 25; i++) {
            ledger.addEntry(buildEntry(10, "entry-" + i));
        }
        offloadRunning.await();

        // trigger a bunch more rolls. Eventually there will be 5 ledgers.
        // first 3 should be offloaded, 4th is 100bytes, 5th is 0 bytes.
        // 4th and 5th sum to 100 bytes so they're just at edge of threshold
        for (int i = 0; i < 20; i++) {
            ledger.addEntry(buildEntry(10, "entry-" + i));
        }

        // allow the first offload to continue
        slowOffload.complete(null);

        assertEventuallyTrue(() -> offloader.offloadedLedgers().size() == 3);
        assertEquals(offloader.offloadedLedgers(),
                            ImmutableSet.of(ledger.getLedgersInfoAsList().get(0).getLedgerId(),
                                            ledger.getLedgersInfoAsList().get(1).getLedgerId(),
                                            ledger.getLedgersInfoAsList().get(2).getLedgerId()));
    }

    @Test
    public void offloadAsSoonAsClosed() throws Exception {
        MockLedgerOffloader offloader = new MockLedgerOffloader();
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        offloader.getOffloadPolicies().setManagedLedgerOffloadThresholdInBytes(0L);
        config.setLedgerOffloader(offloader);

        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);

        for (int i = 0; i < 11; i++) {
            ledger.addEntry(buildEntry(10, "entry-" + i));
        }

        assertEventuallyTrue(() -> offloader.offloadedLedgers().size() == 1);
        assertEquals(offloader.offloadedLedgers(),
                            ImmutableSet.of(ledger.getLedgersInfoAsList().get(0).getLedgerId()));

        for (int i = 0; i < 10; i++) {
            ledger.addEntry(buildEntry(10, "entry-" + i));
        }

        assertEventuallyTrue(() -> offloader.offloadedLedgers().size() == 2);
        assertEquals(offloader.offloadedLedgers(),
                            ImmutableSet.of(ledger.getLedgersInfoAsList().get(0).getLedgerId(),
                                            ledger.getLedgersInfoAsList().get(1).getLedgerId()));
    }


    static void assertEventuallyTrue(BooleanSupplier predicate) throws Exception {
        // wait up to 3 seconds
        for (int i = 0; i < 30 && !predicate.getAsBoolean(); i++) {
            Thread.sleep(100);
        }
        assertTrue(predicate.getAsBoolean());
    }

    static class OffloadCallbackPromise extends CompletableFuture<Position> implements OffloadCallback {
        @Override
        public void offloadComplete(Position pos, Object ctx) {
            complete(pos);
        }

        @Override
        public void offloadFailed(ManagedLedgerException exception, Object ctx) {
            completeExceptionally(exception);
        }
    }

    static class MockLedgerOffloader implements LedgerOffloader {
        interface InjectAfterOffload {
            void call();
        }

        ConcurrentHashMap<Long, UUID> offloads = new ConcurrentHashMap<Long, UUID>();
        ConcurrentHashMap<Long, UUID> deletes = new ConcurrentHashMap<Long, UUID>();
        InjectAfterOffload inject = null;

        Set<Long> offloadedLedgers() {
            return offloads.keySet();
        }

        Set<Long> deletedOffloads() {
            return deletes.keySet();
        }

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
            if (offloads.putIfAbsent(ledger.getId(), uuid) == null) {
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
            CompletableFuture<Void> promise = new CompletableFuture<>();
            if (offloads.remove(ledgerId, uuid)) {
                deletes.put(ledgerId, uuid);
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
    public void testFailByZk() throws Exception {
        MockLedgerOffloader offloader = new MockLedgerOffloader();
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger", config);

        int i = 0;
        for (; i < 25; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        assertEquals(ledger.getLedgersInfoAsList().size(), 3);

        offloader.inject = () -> {
            try {
                stopMetadataStore();
            } catch (Exception e) {
                e.printStackTrace();
            }
        };

        try {
            ledger.offloadPrefix(ledger.getLastConfirmedEntry());
        } catch (Exception e) {

        }
        final LedgerInfo ledgerInfo = ledger.getLedgersInfoAsList().get(0);
        final MLDataFormats.OffloadContext offloadContext = ledgerInfo.getOffloadContext();
        //should not set complete when
        assertEquals(offloadContext.getComplete(), false);
    }

    static class ErroringMockLedgerOffloader extends MockLedgerOffloader {
        CompletableFuture<Set<Long>> errorLedgers = new CompletableFuture<>();

        ErroringMockLedgerOffloader(CompletableFuture<Set<Long>> errorLedgers) {
            this.errorLedgers = errorLedgers;
        }

        @Override
        public CompletableFuture<Void> offload(ReadHandle ledger,
                                               UUID uuid,
                                               Map<String, String> extraMetadata) {
            return errorLedgers.thenCompose(
                            (errors) -> {
                                if (errors.contains(ledger.getId())) {
                                    CompletableFuture<Void> future = new CompletableFuture<>();
                                    future.completeExceptionally(new Exception("Some kind of error"));
                                    return future;
                                } else {
                                    return super.offload(ledger, uuid, extraMetadata);
                                }
                            });
        }
    }
}
