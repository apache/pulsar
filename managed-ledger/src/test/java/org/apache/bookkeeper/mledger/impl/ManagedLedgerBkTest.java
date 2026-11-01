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

import static org.apache.bookkeeper.mledger.util.ManagedLedgerTestUtil.defaultConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.PulsarBookKeeperTestClient;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerAlreadyClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerTerminatedException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.cache.EntryCacheManager;
import org.apache.bookkeeper.mledger.proto.PositionInfo;
import org.apache.bookkeeper.mledger.util.ThrowableToStringUtil;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
public class ManagedLedgerBkTest extends BookKeeperClusterTestCase {

    private final ObjectMapper jackson = new ObjectMapper();

    public ManagedLedgerBkTest() {
        super(2);
        // Use the v2 wire protocol so that reads exercise the BookKeeper batch read API against real bookies
        baseClientConf.setUseV2WireProtocol(true);
    }

    @DataProvider(name = "cacheEvictionModes")
    public Object[][] cacheEvictionModes() {
        return new Object[][]{{false}, {true}};
    }

    @DataProvider
    public Object[][] activationPositionUpdates() {
        return new Object[][]{{false, false}, {false, true}, {true, false}, {true, true}};
    }

    @Test(dataProvider = "cacheEvictionModes", timeOut = 30000)
    public void testActiveSeekDoesNotWaitForLifecycleMonitor(boolean expectedReadCount) throws Exception {
        ManagedLedgerConfig config = defaultConfig()
                .setEnsembleSize(2).setWriteQuorumSize(2).setAckQuorumSize(2);
        config.setCacheEvictionByExpectedReadCount(expectedReadCount);
        config.setCacheEvictionByMarkDeletedPosition(false);
        config.setReadAheadEnabled(false);
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(
                "active-cursor-position-" + UUID.randomUUID(), config);
        ManagedCursor cursor = ledger.openCursor("cursor");
        Position target = ledger.addEntry(new byte[]{1});
        assertThat(cursor.isActive()).isTrue();
        ExecutorService caller = Executors.newSingleThreadExecutor(
                new DefaultThreadFactory("active-cursor-position", true));
        try {
            synchronized (ledger.getActiveCursors()) {
                // Ordinary reads update an already active cursor without taking the lifecycle monitor.
                Future<?> seek = caller.submit(() -> cursor.seek(target));
                seek.get(10, TimeUnit.SECONDS);
            }
            assertThat(ledger.getActiveCursors().getSlowestCursorPosition()).isEqualTo(target);
        } finally {
            caller.shutdownNow();
            assertThat(caller.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test(dataProvider = "activationPositionUpdates", timeOut = 30000)
    public void testActivationPreservesConcurrentSeek(boolean expectedReadCount, boolean moveForward) throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc) {
            @Override
            protected ManagedLedgerImpl createManagedLedger(BookKeeper bk, MetaStore store, String name,
                    ManagedLedgerConfig config, Supplier<CompletableFuture<Boolean>> ownershipChecker) {
                return new ManagedLedgerImpl(this, bk, store, config, scheduledExecutor, name, ownershipChecker) {
                    @Override
                    protected ManagedCursorImpl createCursor(BookKeeper bookKeeper, String cursorName) {
                        return new PausingReadPositionCursor(bookKeeper, this, cursorName);
                    }
                };
            }
        };
        ManagedLedgerConfig config = defaultConfig()
                .setEnsembleSize(2).setWriteQuorumSize(2).setAckQuorumSize(2);
        config.setCacheEvictionByExpectedReadCount(expectedReadCount);
        config.setCacheEvictionByMarkDeletedPosition(false);
        config.setReadAheadEnabled(false);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(
                "cursor-activation-position-" + UUID.randomUUID(), config);
        PausingReadPositionCursor cursor = (PausingReadPositionCursor) ledger.openCursor("moving");
        ManagedCursor reference = ledger.openCursor("reference");
        List<Position> positions = new ArrayList<>();
        for (int index = 0; index < 5; index++) {
            positions.add(ledger.addEntry(new byte[]{(byte) index}));
        }
        reference.seek(positions.get(2));
        cursor.setInactive();
        cursor.seek(positions.get(moveForward ? 0 : 4));
        Position target = positions.get(moveForward ? 4 : 0);
        cursor.pauseNextSnapshot.set(true);
        AtomicReference<Thread> seekThread = new AtomicReference<>();
        ExecutorService callers = Executors.newFixedThreadPool(2,
                new DefaultThreadFactory("cursor-activation-position", true));
        try {
            Future<?> activation = callers.submit(cursor::setActive);
            assertThat(cursor.positionSampled.await(10, TimeUnit.SECONDS)).isTrue();
            Future<?> seek = callers.submit(() -> {
                seekThread.set(Thread.currentThread());
                cursor.seek(target);
            });
            // Before the fix the notification finishes without finding the cursor. With the fix it waits
            // for registration's container lock. Wait for either outcome, not a scheduling-dependent delay.
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> seek.isDone()
                    || (seekThread.get() != null && (seekThread.get().getState() == Thread.State.WAITING
                    || seekThread.get().getState() == Thread.State.BLOCKED)));
            assertThat(cursor.getReadPosition()).isEqualTo(target);
            cursor.resumeSnapshot.countDown();
            activation.get(10, TimeUnit.SECONDS);
            seek.get(10, TimeUnit.SECONDS);

            assertThat(cursor.isActive()).isTrue();
            assertThat(ledger.isCursorActive(cursor)).isTrue();
            ActiveManagedCursorContainer activeCursors = ledger.getActiveCursors();
            if (expectedReadCount) {
                assertThat(ledger.getNumberOfCursorsAtSamePositionOrBefore(reference))
                        .as("expected reader count at the reference cursor")
                        .isEqualTo(moveForward ? 1 : 2);
                assertThat(ledger.getNumberOfCursorsAtSamePositionOrBefore(cursor))
                        .isEqualTo(moveForward ? 2 : 1);
            }
            assertThat(activeCursors.getSlowestCursorPosition())
                    .as("registration must not lose the concurrent seek")
                    .isEqualTo(moveForward ? reference.getReadPosition() : target);
        } finally {
            cursor.resumeSnapshot.countDown();
            callers.shutdownNow();
            assertThat(callers.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test(dataProvider = "activationPositionUpdates", timeOut = 30000)
    public void testActivationIgnoresDelayedReadPositionUpdate(boolean expectedReadCount, boolean moveForward)
            throws Exception {
        assertDelayedReadPositionUpdate(expectedReadCount, moveForward, false);
    }

    @Test(dataProvider = "activationPositionUpdates", timeOut = 30000)
    public void testDelayedReadPositionUpdateReconcilesConcurrentSeek(boolean expectedReadCount, boolean moveForward)
            throws Exception {
        assertDelayedReadPositionUpdate(expectedReadCount, moveForward, true);
    }

    private void assertDelayedReadPositionUpdate(boolean expectedReadCount, boolean moveForward,
                                                boolean overlapActiveSeek) throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc) {
            @Override
            protected ManagedLedgerImpl createManagedLedger(BookKeeper bk, MetaStore store, String name,
                    ManagedLedgerConfig config, Supplier<CompletableFuture<Boolean>> ownershipChecker) {
                return new ManagedLedgerImpl(this, bk, store, config, scheduledExecutor, name, ownershipChecker) {
                    @Override
                    protected ManagedCursorImpl createCursor(BookKeeper bookKeeper, String cursorName) {
                        return new PausingReadPositionCursor(bookKeeper, this, cursorName);
                    }
                };
            }
        };
        ManagedLedgerConfig config = defaultConfig()
                .setEnsembleSize(2).setWriteQuorumSize(2).setAckQuorumSize(2);
        config.setCacheEvictionByExpectedReadCount(expectedReadCount);
        config.setCacheEvictionByMarkDeletedPosition(false);
        config.setReadAheadEnabled(false);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(
                "delayed-cursor-position-" + UUID.randomUUID(), config);
        PausingReadPositionCursor cursor = (PausingReadPositionCursor) ledger.openCursor("moving");
        ManagedCursor reference = ledger.openCursor("reference");
        List<Position> positions = new ArrayList<>();
        for (int index = 0; index < 5; index++) {
            positions.add(ledger.addEntry(new byte[]{(byte) index}));
        }
        reference.seek(positions.get(2));
        cursor.setInactive();
        cursor.seek(positions.get(2));
        Position oldPosition = positions.get(moveForward ? 0 : 4);
        Position target = positions.get(moveForward ? 4 : 0);
        AtomicReference<Thread> readThread = new AtomicReference<>();
        ExecutorService caller = Executors.newSingleThreadExecutor(
                new DefaultThreadFactory("delayed-cursor-position", true));
        try {
            Future<?> read;
            synchronized (ledger.getActiveCursors()) {
                read = caller.submit(() -> {
                    readThread.set(Thread.currentThread());
                    // Use the same position publication/notification method as OpReadEntry's completion.
                    cursor.setReadPosition(oldPosition);
                });
                Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> readThread.get() != null
                        && readThread.get().getState() == Thread.State.BLOCKED);
                assertThat(cursor.getReadPosition()).isEqualTo(oldPosition);
                // Publish and register a newer position before allowing the old notification to resume.
                cursor.seek(overlapActiveSeek ? positions.get(moveForward ? 1 : 3) : target);
                cursor.setActive();
                if (overlapActiveSeek) {
                    cursor.pauseNextSnapshot.set(true);
                }
            }
            if (overlapActiveSeek) {
                assertThat(cursor.positionSampled.await(10, TimeUnit.SECONDS)).isTrue();
                // The active path can overtake reconciliation after it has sampled the current position.
                cursor.seek(target);
                cursor.resumeSnapshot.countDown();
            }
            read.get(10, TimeUnit.SECONDS);

            assertThat(cursor.getReadPosition()).isEqualTo(target);
            assertThat(cursor.isActive()).isTrue();
            assertThat(ledger.isCursorActive(cursor)).isTrue();
            if (expectedReadCount) {
                assertThat(ledger.getNumberOfCursorsAtSamePositionOrBefore(reference))
                        .as("a delayed notification must not overwrite the latest reader count")
                        .isEqualTo(moveForward ? 1 : 2);
                assertThat(ledger.getNumberOfCursorsAtSamePositionOrBefore(cursor))
                        .isEqualTo(moveForward ? 2 : 1);
            }
            assertThat(ledger.getActiveCursors().getSlowestCursorPosition())
                    .as("a delayed notification must not overwrite the latest position")
                    .isEqualTo(moveForward ? reference.getReadPosition() : target);
        } finally {
            cursor.resumeSnapshot.countDown();
            caller.shutdownNow();
            assertThat(caller.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static final class PausingReadPositionCursor extends ManagedCursorImpl {
        private final AtomicBoolean pauseNextSnapshot = new AtomicBoolean();
        private final CountDownLatch positionSampled = new CountDownLatch(1);
        private final CountDownLatch resumeSnapshot = new CountDownLatch(1);

        private PausingReadPositionCursor(BookKeeper bookKeeper, ManagedLedgerImpl ledger, String name) {
            super(bookKeeper, ledger, name);
        }

        @Override
        public Position getReadPosition() {
            Position position = super.getReadPosition();
            if (pauseNextSnapshot.compareAndSet(true, false)) {
                positionSampled.countDown();
                try {
                    if (!resumeSnapshot.await(10, TimeUnit.SECONDS)) {
                        throw new AssertionError("activation position snapshot was not resumed");
                    }
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("activation interrupted", error);
                }
            }
            return position;
        }
    }

    @DataProvider
    public Object[][] addEntryHandoverByteLimits() {
        return new Object[][] {
                {new int[] {4, 4, 4}, 2},
                {new int[] {4, 100, 4}, 1},
                {new int[] {100, 4}, 1},
                {new int[] {4, 6, 4}, 2}
        };
    }

    @Test(timeOut = 30000, dataProvider = "addEntryHandoverByteLimits")
    public void testAddEntryHandoverYieldsBeforeExceedingByteLimit(int[] entrySizes, int firstBatchEntries)
            throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = defaultConfig()
                .setEnsembleSize(2).setWriteQuorumSize(2).setAckQuorumSize(2)
                .setAddEntryHandoverMaxBatchBytesSize(10);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("handover-byte-limit-" + UUID.randomUUID(), config);
        ManagedCursor cursor = ledger.openCursor("c1");
        CountDownLatch workerStarted = new CountDownLatch(1);
        CountDownLatch resumeWorker = new CountDownLatch(1);
        CompletableFuture<Long> entriesBeforeOtherWork = new CompletableFuture<>();
        List<CompletableFuture<Position>> positions = new ArrayList<>();
        try {
            // Hold the real BookKeeper worker until all adds and the competing task have been queued.
            ledger.getExecutor().execute(() -> {
                workerStarted.countDown();
                try {
                    if (!resumeWorker.await(10, TimeUnit.SECONDS)) {
                        throw new AssertionError("Timed out waiting to resume the BookKeeper worker");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            });
            assertThat(workerStarted.await(10, TimeUnit.SECONDS)).isTrue();
            for (int i = 0; i < entrySizes.length; i++) {
                CompletableFuture<Position> position = new CompletableFuture<>();
                positions.add(position);
                ByteBuf data = Unpooled.buffer(entrySizes[i]).writeZero(entrySizes[i]);
                data.setByte(0, i);
                try {
                    ledger.asyncAddEntry(data, new AddEntryCallback() {
                        @Override
                        public void addComplete(Position addedPosition, ByteBuf entryData, Object ctx) {
                            position.complete(addedPosition);
                        }

                        @Override
                        public void addFailed(ManagedLedgerException exception, Object ctx) {
                            position.completeExceptionally(exception);
                        }
                    }, null);
                } finally {
                    data.release();
                }
            }
            // This task must run between the first batch and the adds that do not fit in it.
            ledger.getExecutor().execute(() -> entriesBeforeOtherWork.complete(ledger.getCurrentLedgerEntries()));
        } finally {
            resumeWorker.countDown();
        }

        assertThat(entriesBeforeOtherWork.get(10, TimeUnit.SECONDS)).isEqualTo(firstBatchEntries);
        List<Position> added = new ArrayList<>();
        for (CompletableFuture<Position> position : positions) {
            added.add(position.get(10, TimeUnit.SECONDS));
        }
        assertThat(added).isSorted().doesNotHaveDuplicates();
        List<Entry> entries = cursor.readEntries(entrySizes.length);
        try {
            assertThat(entries).hasSize(entrySizes.length);
            for (int i = 0; i < entrySizes.length; i++) {
                assertThat(entries.get(i).getLength()).isEqualTo(entrySizes[i]);
                assertThat(entries.get(i).getDataBuffer().getByte(0)).isEqualTo((byte) i);
                assertThat(entries.get(i).getPosition()).isEqualTo(added.get(i));
            }
        } finally {
            entries.forEach(Entry::release);
        }
    }

    @Test
    public void testBatchReadNotUsedWithV3WireProtocolClient() throws Exception {
        // The suite's client uses the v2 wire protocol; a v3 client cannot batch read, so reads must not use it
        ClientConfiguration v3ClientConf = new ClientConfiguration(baseClientConf);
        v3ClientConf.setUseV2WireProtocol(false);
        @Cleanup
        BookKeeper v3Client = new PulsarBookKeeperTestClient(v3ClientConf);
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, v3Client);
        ManagedLedgerConfig config = defaultConfig();
        config.setEnsembleSize(2).setAckQuorumSize(2);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger" + testName, config);
        assertFalse(ledger.isBatchReadEnabled());
        ManagedCursor cursor = ledger.openCursor("c1");
        for (int i = 0; i < 10; i++) {
            ledger.addEntry(("entry-" + i).getBytes());
        }
        List<Entry> entries = cursor.readEntries(10);
        assertEquals(entries.size(), 10);
        entries.forEach(Entry::release);
    }

    @Test
    public void testEstimatedUnackedSizeDuringRolloverWithPendingAdd() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = defaultConfig()
                .setEnsembleSize(2).setWriteQuorumSize(2).setAckQuorumSize(2)
                .setRetentionSizeInMB(-1).setRetentionTime(-1, TimeUnit.MILLISECONDS);
        config.setMinimumRolloverTime(0, TimeUnit.MILLISECONDS);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("pending-add-" + testName, config);
        ManagedCursor cursor = ledger.openCursor("c1");
        byte[] data = "entry".getBytes(StandardCharsets.UTF_8);
        ledger.addEntry(data);
        Position lastPosition = ledger.addEntry(data);
        cursor.markDelete(lastPosition);
        config.setMaxEntriesPerLedger(2);

        CompletableFuture<Position> pendingAdd = new CompletableFuture<>();
        CountDownLatch resumeBookie = new CountDownLatch(1);
        CountDownLatch bookieSuspended = new CountDownLatch(1);
        try {
            // Both bookies must acknowledge the write. Hold one so the first add to the new ledger stays pending.
            sleepBookie(getBookie(0), resumeBookie, bookieSuspended);
            assertThat(bookieSuspended.await(10, TimeUnit.SECONDS)).isTrue();
            ledger.rollCurrentLedgerIfFull();
            ledger.asyncAddEntry(data, new AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    pendingAdd.complete(position);
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    pendingAdd.completeExceptionally(exception);
                }
            }, null);

            // Rollover itself moves the caught-up cursor to the new ledger's -1 sentinel.
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(cursor.getMarkDeletedPosition().getLedgerId()).isGreaterThan(lastPosition.getLedgerId());
                assertThat(cursor.getMarkDeletedPosition().getEntryId()).isEqualTo(-1);
                assertThat(ledger.getCurrentLedgerEntries()).isEqualTo(1);
            });
            assertThat(cursor.getMarkDeletedPosition().getLedgerId()).isEqualTo(ledger.currentLedger.getId());
            assertThat(ledger.getLastPosition()).isEqualTo(lastPosition);
            // Keep the old ledger so the removed-ledger shortcut cannot mask the regression.
            assertThat(ledger.ledgerExists(lastPosition.getLedgerId())).isTrue();
            assertThat(pendingAdd).isNotDone();
            assertThat(cursor.getEstimatedSizeSinceMarkDeletePosition()).isZero();
        } finally {
            resumeBookie.countDown();
        }

        Position newPosition = pendingAdd.get(10, TimeUnit.SECONDS);
        assertThat(newPosition.getLedgerId()).isGreaterThan(lastPosition.getLedgerId());
        assertThat(newPosition.getEntryId()).isZero();
        assertThat(cursor.getEstimatedSizeSinceMarkDeletePosition()).isEqualTo(data.length);
        List<Entry> entries = cursor.readEntries(1);
        try {
            assertThat(entries).hasSize(1);
            assertThat(entries.get(0).getPosition()).isEqualTo(newPosition);
            assertThat(entries.get(0).getData()).isEqualTo(data);
        } finally {
            entries.forEach(Entry::release);
        }
        cursor.markDelete(newPosition);
        assertThat(cursor.getEstimatedSizeSinceMarkDeletePosition()).isZero();
    }

    @Test
    public void testEstimatedUnackedSizeRejectsUnconfirmedEntryInEmptyLedger() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = defaultConfig()
                .setEnsembleSize(2).setWriteQuorumSize(2).setAckQuorumSize(2)
                .setRetentionSizeInMB(-1).setRetentionTime(-1, TimeUnit.MILLISECONDS);
        config.setMinimumRolloverTime(0, TimeUnit.MILLISECONDS);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("invalid-position-" + testName, config);
        ManagedCursor cursor = ledger.openCursor("c1");
        Position lastPosition = ledger.addEntry("entry".getBytes(StandardCharsets.UTF_8));
        cursor.markDelete(lastPosition);
        config.setMaxEntriesPerLedger(1);
        ledger.rollCurrentLedgerIfFull();

        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(cursor.getMarkDeletedPosition().getLedgerId()).isGreaterThan(lastPosition.getLedgerId());
            assertThat(cursor.getMarkDeletedPosition().getEntryId()).isEqualTo(-1);
        });
        assertThat(cursor.getEstimatedSizeSinceMarkDeletePosition()).isZero();

        // asyncMarkDelete currently accepts an ahead-of-LAC entry in the next ledger after consuming the old one.
        // The estimator must not mistake that entry for the valid -1 sentinel just because the ledger is empty.
        Position invalidPosition = PositionFactory.create(cursor.getMarkDeletedPosition().getLedgerId(), 0);
        cursor.markDelete(invalidPosition);
        assertThat(cursor.getMarkDeletedPosition()).isEqualTo(invalidPosition);
        assertThat(ledger.getCurrentLedgerEntries()).isZero();
        assertThat(ledger.getLastPosition()).isEqualTo(lastPosition);
        assertThat(ledger.ledgerExists(lastPosition.getLedgerId())).isTrue();
        assertThatThrownBy(cursor::getEstimatedSizeSinceMarkDeletePosition)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is ahead of the last position");
    }

    @Test
    public void testSimpleRead() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);

        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConf);
        ManagedLedgerConfig config = defaultConfig();
        config.setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1);
        ManagedLedger ledger = factory.open("my-ledger" + testName, config);
        ManagedCursor cursor = ledger.openCursor("c1");

        int num = 1;

        for (int i = 0; i < num; i++) {
            String entry = "entry-" + i;
            ledger.addEntry(entry.getBytes());
        }

        List<Entry> entries = cursor.readEntries(num);
        assertEquals(num, entries.size());
        entries.forEach(Entry::release);
    }

    @Test
    public void testSeekRetracksCursorAfterUntracking() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = defaultConfig()
                .setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1);
        config.setCacheEvictionByExpectedReadCount(true);
        config.setCacheEvictionByMarkDeletedPosition(false);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("cursor-untracking-" + testName, config);
        List<Position> positions = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            positions.add(ledger.addEntry(("entry-" + i).getBytes(StandardCharsets.UTF_8)));
        }
        List<ManagedCursor> cursors = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            ManagedCursor cursor = ledger.openCursor("cursor" + i, InitialPosition.Earliest);
            cursor.seek(positions.get(i + 1));
            cursor.setActive();
            cursors.add(cursor);
        }
        ActiveManagedCursorContainer container = ledger.getActiveCursors();
        assertThat(container.getSlowestCursorPosition()).isEqualTo(positions.get(1));
        ManagedCursor cursor = cursors.get(2);
        // Null untracking is a container API operation; normal broker deactivation removes the cursor.
        container.updateCursor(cursor, null);
        assertThat(container.getSlowestCursorPosition()).isEqualTo(positions.get(1));

        // Exercise ManagedCursorImpl -> ManagedLedgerImpl -> the active cursor container.
        // One update among four tracked nodes forces incremental reinsertion.
        cursor.seek(positions.get(6));
        assertThat(ledger.getNumberOfCursorsAtSamePositionOrBefore(cursor)).isEqualTo(5);
        assertThat(container.getSlowestCursorPosition()).isEqualTo(positions.get(1));
        assertThat(container.size()).isEqualTo(5);
        for (int i = 0; i < cursors.size(); i++) {
            if (i != 2) {
                assertThat(ledger.getNumberOfCursorsAtSamePositionOrBefore(cursors.get(i)))
                        .as("rank of cursor%s", i).isEqualTo(i < 2 ? i + 1 : i);
            }
        }
    }

    @Test
    public void testBookieFailure() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = defaultConfig();
        config.setEnsembleSize(2).setAckQuorumSize(2);
        ManagedLedger ledger = factory.open("my-ledger" + testName, config);
        ManagedCursor cursor = ledger.openCursor("my-cursor");
        ledger.addEntry("entry-0".getBytes());

        killBookie(1);

        // Now we want to simulate that:
        // 1. The write operation fails because we only have 1 bookie available
        // 2. The bk client cannot properly close the ledger (finalizing the number of entries) because ZK is also
        // not available
        // 3. When we re-establish the service one, the ledger recovery will be triggered and the half-committed entry
        // is restored

        // Force to close the ZK client object so that BK will fail to close the ledger
        bkc.getZkHandle().close();

        try {
            ledger.addEntry("entry-1".getBytes());
            fail("should fail");
        } catch (ManagedLedgerException e) {
            // ok
        }

        bkc.close();
        metadataStore.unsetAlwaysFail();

        bkc = new PulsarBookKeeperTestClient(baseClientConf);
        startNewBookie();

        // Reconnect a new bk client
        factory.shutdown();

        factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory.open("my-ledger" + testName, config);
        cursor = ledger.openCursor("my-cursor");

        // Next add should succeed
        ledger.addEntry("entry-2".getBytes());

        assertEquals(3, cursor.getNumberOfEntriesInBacklog(false));

        List<Entry> entries = cursor.readEntries(1);
        assertEquals(1, entries.size());
        assertEquals("entry-0", new String(entries.get(0).getData()));
        entries.forEach(Entry::release);

        // entry-1 which was half-committed will get fully committed during the recovery phase
        entries = cursor.readEntries(1);
        assertEquals(1, entries.size());
        assertEquals("entry-1", new String(entries.get(0).getData()));
        entries.forEach(Entry::release);

        entries = cursor.readEntries(1);
        assertEquals(1, entries.size());
        assertEquals("entry-2", new String(entries.get(0).getData()));
        entries.forEach(Entry::release);
        factory.shutdown();
    }

    @Test
    public void verifyConcurrentUsage() throws Exception {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();

        config.setMaxCacheSize(100 * 1024 * 1024);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, config);

        EntryCacheManager cacheManager = factory.getEntryCacheManager();
        ManagedLedgerConfig conf = defaultConfig();
        conf.setEnsembleSize(2).setAckQuorumSize(2);
        final ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my-ledger-" + UUID.randomUUID(), conf);

        int numProducers = 1;
        int numConsumers = 1;

        final AtomicBoolean done = new AtomicBoolean();
        final CyclicBarrier barrier = new CyclicBarrier(numProducers + numConsumers + 1);

        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < numProducers; i++) {
            futures.add(executor.submit(() -> {
                try {
                    barrier.await();

                    while (!done.get()) {
                        ledger.addEntry("entry".getBytes());
                        Thread.sleep(1);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }));
        }

        for (int i = 0; i < numConsumers; i++) {
            final int idx = i;
            futures.add(executor.submit(() -> {
                try {
                    // Open the cursor before any entry is added: an entry appended while the cursor is being created
                    // can be skipped by the cache and still land after the cursor's starting position
                    ManagedCursor cursor = ledger.openCursor("my-cursor-" + idx);

                    barrier.await();

                    while (!done.get()) {
                        List<Entry> entries = cursor.readEntries(1);
                        if (!entries.isEmpty()) {
                            cursor.markDelete(entries.get(0).getPosition());
                        }

                        entries.forEach(Entry::release);
                        Thread.sleep(2);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }));
        }

        barrier.await();

        Thread.sleep(1 * 1000);

        done.set(true);
        for (Future<?> future : futures) {
            future.get();
        }

        factory.getMbean().refreshStats(1, TimeUnit.SECONDS);

        assertTrue(factory.getMbean().getCacheHitsRate() > 0.0);
        assertEquals(factory.getMbean().getCacheMissesRate(), 0.0);
        assertTrue(factory.getMbean().getCacheHitsThroughput() > 0.0);
        assertEquals(factory.getMbean().getNumberOfCacheEvictions(), 0);
    }

    @Test
    public void verifyAsyncReadEntryUsingCache() throws Exception {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();

        config.setMaxCacheSize(100 * 1024 * 1024);
        config.setCacheEvictionTimeThresholdMillis(10000);
        config.setCacheEvictionIntervalMs(10000);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, config);

        ManagedLedgerConfig conf = defaultConfig();
        conf.setEnsembleSize(2).setAckQuorumSize(2)
                .setRetentionSizeInMB(-1).setRetentionTime(-1, TimeUnit.MILLISECONDS);
        final ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my-ledger" + testName, conf);

        int numProducers = 5;
        int numConsumers = 10;

        final AtomicBoolean done = new AtomicBoolean();
        final CyclicBarrier barrier = new CyclicBarrier(numProducers + numConsumers + 1);

        List<Future<?>> futures = new ArrayList<>();
        List<Position> positions = new CopyOnWriteArrayList<>();

        for (int i = 0; i < numProducers; i++) {
            futures.add(executor.submit(() -> {
                try {
                    // wait for all threads to be ready to start at once
                    barrier.await();
                    while (!done.get()) {
                        Position position = ledger.addEntry("entry".getBytes());
                        positions.add(position);
                        Thread.sleep(1);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    throw FutureUtil.wrapToCompletionException(e);
                }
            }));
        }

        // create a dummy cursor since caching happens only when there are active consumers
        ManagedCursor cursor = ledger.openCursor("dummy");

        for (int i = 0; i < numConsumers; i++) {
            futures.add(executor.submit(() -> {
                try {
                    // wait for all threads to be ready to start at once
                    barrier.await();
                    while (!done.get()) {
                        if (positions.isEmpty()) {
                            Thread.sleep(1);
                            continue;
                        }
                        // Simulate a replay queue read pattern where individual entries are read
                        Position randomPosition = positions.get(ThreadLocalRandom.current().nextInt(positions.size()));
                        // Clone the original instance so that another instance is used in the asyncReadEntry call
                        // This is to test that keys are compared by .equals and not by reference under the covers
                        randomPosition = PositionFactory.create(randomPosition);
                        CompletableFuture<Void> future = new CompletableFuture<>();
                        ledger.asyncReadEntry(randomPosition, new AsyncCallbacks.ReadEntryCallback() {
                            @Override
                            public void readEntryComplete(Entry entry, Object ctx) {
                                entry.release();
                                future.complete(null);
                            }

                            @Override
                            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                                future.completeExceptionally(exception);
                            }
                        }, null);
                        future.get();
                        Thread.sleep(2);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    throw FutureUtil.wrapToCompletionException(e);
                }
            }));
        }

        // trigger all worker threads at once to continue from the barrier
        barrier.await();

        int testDurationSeconds = 3;
        Thread.sleep(testDurationSeconds * 1000);

        done.set(true);
        for (Future<?> future : futures) {
            future.get();
        }

        factory.getMbean().refreshStats(testDurationSeconds, TimeUnit.SECONDS);

        assertTrue(factory.getMbean().getCacheHitsRate() > 0.0);
        assertEquals(factory.getMbean().getCacheMissesRate(), 0.0);
        assertTrue(factory.getMbean().getCacheHitsThroughput() > 0.0);
        assertEquals(factory.getMbean().getNumberOfCacheEvictions(), 0);
    }

    @Test
    public void testSimple() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig mlConfig = defaultConfig();
        mlConfig.setEnsembleSize(1).setAckQuorumSize(1).setWriteQuorumSize(1);
        // set the data ledger size
        mlConfig.setMaxEntriesPerLedger(100);
        // set the metadata ledger size to 1 to kick off many ledger switching cases
        mlConfig.setMetadataMaxEntriesPerLedger(2);
        ManagedLedger ledger = factory.open("ml-simple-ledger", mlConfig);

        ledger.addEntry("test".getBytes());
    }

    @Test
    public void testConcurrentMarkDelete() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig mlConfig = defaultConfig();
        mlConfig.setEnsembleSize(1).setWriteQuorumSize(1)
            .setAckQuorumSize(1);
        // set the data ledger size
        mlConfig.setMaxEntriesPerLedger(100);
        // set the metadata ledger size to 1 to kick off many ledger switching cases
        mlConfig.setMetadataMaxEntriesPerLedger(10);
        ManagedLedger ledger = factory.open("ml-markdelete-ledger", mlConfig);

        final List<Position> addedEntries = new ArrayList<>();

        int numCursors = 10;
        final CyclicBarrier barrier = new CyclicBarrier(numCursors);

        List<ManagedCursor> cursors = new ArrayList<>();
        for (int i = 0; i < numCursors; i++) {
            cursors.add(ledger.openCursor(String.format("c%d", i)));
        }

        for (int i = 0; i < 50; i++) {
            Position pos = ledger.addEntry("entry".getBytes());
            addedEntries.add(pos);
        }

        List<Future<?>> futures = new ArrayList<>();

        for (ManagedCursor cursor : cursors) {
            futures.add(executor.submit(() -> {
                barrier.await();

                for (Position position : addedEntries) {
                    cursor.markDelete(position);
                }

                return null;
            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }

        // Since in this test we roll-over the cursor ledger every 10 entries acknowledged, the background roll back
        // might still be happening when the futures are completed.
        Thread.sleep(1000);
    }

    @Test
    public void asyncMarkDeleteAndClose() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);

        ManagedLedgerConfig config = defaultConfig().setEnsembleSize(1).setWriteQuorumSize(1)
                .setAckQuorumSize(1);
        ManagedLedger ledger = factory.open("my_test_ledger" + testName, config);
        ManagedCursor cursor = ledger.openCursor("c1");

        List<Position> positions = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            Position p = ledger.addEntry("entry".getBytes());
            positions.add(p);
        }

        final CountDownLatch counter = new CountDownLatch(positions.size());
        final AtomicReference<Exception> gotException = new AtomicReference<>();

        for (Position p : positions) {
            cursor.asyncDelete(p, new DeleteCallback() {
                @Override
                public void deleteComplete(Object ctx) {
                    // Ok
                    counter.countDown();
                }

                @Override
                public void deleteFailed(ManagedLedgerException exception, Object ctx) {
                    gotException.set(exception);
                    counter.countDown();
                }
            }, null);
        }

        counter.await();

        // cleanup.
        closeManagedLedgerWithRetry(ledger);

        // Add information to determine the problem.
        if (gotException.get() != null){
            fail(ThrowableToStringUtil.toString(gotException.get()));
        }
    }

    /**
     * When auto-replication is triggered, if there were no writes on the ML during the grace period, auto-replication
     * will close the ledger an re-replicate it. After that, the next write will get a FencedException. We should
     * recover from this condition by creating a new ledger and retrying the write.
     */
    @Test
    public void ledgerFencedByAutoReplication() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = defaultConfig();
        config.setEnsembleSize(2).setAckQuorumSize(2);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger" + testName, config);
        ManagedCursor c1 = ledger.openCursor("c1");

        Position p1 = ledger.addEntry("entry-1".getBytes());

        // Trigger the closure of the data ledger
        bkc.openLedger(p1.getLedgerId(), BookKeeper.DigestType.CRC32C, new byte[] {});

        ledger.addEntry("entry-2".getBytes());

        assertEquals(2, c1.getNumberOfEntries());
        assertEquals(2, c1.getNumberOfEntriesInBacklog(false));

        Position p3 = ledger.addEntry("entry-3".getBytes());

        // Now entry-2 should have been written before entry-3
        assertEquals(3, c1.getNumberOfEntries());
        assertEquals(3, c1.getNumberOfEntriesInBacklog(false));
        assertTrue(p1.getLedgerId() != p3.getLedgerId());
    }

    @Test
    public void testLedgerCallbacksRunOnManagedLedgerThread() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = defaultConfig().setMaxEntriesPerLedger(2)
                .setEnsembleSize(2).setWriteQuorumSize(2).setAckQuorumSize(2);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("ml-thread-callbacks-" + UUID.randomUUID(), config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("c1");
        Thread mlThread = CompletableFuture.supplyAsync(Thread::currentThread, ledger.getExecutor()).get();

        // Data ledger created with the managed ledger name as ordering key
        Position p1 = ledger.addEntry("entry-1".getBytes(StandardCharsets.UTF_8));
        assertEquals(readCallbackThread(ledger.currentLedger, p1.getEntryId()), mlThread);

        // Closed ledger re-opened through getLedgerHandle with the same key
        ledger.addEntry("entry-2".getBytes(StandardCharsets.UTF_8));
        Position p3 = ledger.addEntry("entry-3".getBytes(StandardCharsets.UTF_8));
        assertNotEquals(p3.getLedgerId(), p1.getLedgerId());
        LedgerHandle reopened = (LedgerHandle) ledger.getLedgerHandle(p1.getLedgerId()).get(10, TimeUnit.SECONDS);
        assertEquals(readCallbackThread(reopened, p1.getEntryId()), mlThread);

        // Cursor ledger, created through the same create path
        cursor.markDelete(p1);
        Awaitility.await().until(() -> cursor.cursorLedger != null && cursor.cursorLedger.getLastAddConfirmed() >= 0);
        assertEquals(readCallbackThread(cursor.cursorLedger, cursor.cursorLedger.getLastAddConfirmed()), mlThread);
    }

    @Test
    public void testInlineAddCompletionsKeepOrder() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig()
                .setEnsembleSize(2).setWriteQuorumSize(2).setAckQuorumSize(2);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("inline-add-order-" + UUID.randomUUID(), config);

        int entries = 200;
        List<Position> completed = new CopyOnWriteArrayList<>();
        AtomicReference<ManagedLedgerException> failure = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(entries);
        AddEntryCallback callback = new AddEntryCallback() {
            @Override
            public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                completed.add(position);
                latch.countDown();
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                failure.compareAndSet(null, exception);
                latch.countDown();
            }
        };
        // Submit from the managed ledger thread, so the BookKeeper completions and their processing all run
        // inline on that thread, and check they still complete the adds in submission order
        ledger.getExecutor().execute(() -> {
            for (int i = 0; i < entries; i++) {
                ledger.asyncAddEntry(("entry-" + i).getBytes(StandardCharsets.UTF_8), callback, null);
            }
        });
        assertTrue(latch.await(30, TimeUnit.SECONDS));
        assertNull(failure.get());
        assertEquals(completed.size(), entries);
        for (int i = 1; i < entries; i++) {
            assertTrue(completed.get(i - 1).compareTo(completed.get(i)) < 0, "completed out of order at " + i);
        }
    }

    private static Thread readCallbackThread(LedgerHandle lh, long entryId) throws Exception {
        CompletableFuture<Thread> callbackThread = new CompletableFuture<>();
        lh.asyncReadEntries(entryId, entryId,
                (rc, handle, seq, ctx) -> callbackThread.complete(Thread.currentThread()), null);
        return callbackThread.get(10, TimeUnit.SECONDS);
    }

    /**
     * When another process steals the ML, the old instance should not succeed in any operation.
     */
    @Test
    public void ledgerFencedByFailover() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory1 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = defaultConfig();
        config.setEnsembleSize(2).setAckQuorumSize(2);
        ManagedLedgerImpl ledger1 = (ManagedLedgerImpl) factory1.open("my_test_ledger" + testName, config);
        ledger1.openCursor("c");

        ledger1.addEntry("entry-1".getBytes());

        // Open the ML from another factory
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerImpl ledger2 = (ManagedLedgerImpl) factory2.open("my_test_ledger" + testName, config);
        ManagedCursor c2 = ledger2.openCursor("c");

        try {
            ledger1.addEntry("entry-2".getBytes());
            fail("Should have failed");
        } catch (ManagedLedgerException e) {
            // Ok
        }

        ledger2.addEntry("entry-2".getBytes());

        try {
            ledger1.addEntry("entry-2".getBytes());
            fail("Should have failed");
        } catch (ManagedLedgerException bve) {
            // Ok
        }

        assertEquals(2, c2.getNumberOfEntriesInBacklog(false));
    }

    @Test
    public void testOfflineTopicBacklog() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);

        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConf);
        ManagedLedgerConfig config = defaultConfig();
        config.setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1).setDigestType(DigestType.CRC32);
        TopicName topicName = TopicName.get("persistent://property/namespace/my-ledger");
        ManagedLedger ledger = factory.open(topicName.getPersistenceNamingEncoding(), config);
        ManagedCursor cursor = ledger.openCursor("c1");

        int num = 3;

        for (int i = 0; i < num; i++) {
            String entry = "entry-" + i;
            ledger.addEntry(entry.getBytes());
        }

        List<Entry> entries = cursor.readEntries(num);
        assertEquals(num, entries.size());
        entries.forEach(Entry::release);
        ledger.close();

        ManagedLedgerOfflineBacklog offlineTopicBacklog = new ManagedLedgerOfflineBacklog(
                DigestType.CRC32, "".getBytes(StandardCharsets.UTF_8), "", false);
        PersistentOfflineTopicStats offlineTopicStats = offlineTopicBacklog.estimateUnloadedTopicBacklog(
                factory, topicName);
        assertNotNull(offlineTopicStats);
        assertEquals(offlineTopicStats.totalMessages, num);
        assertEquals(offlineTopicStats.messageBacklog, num);
        assertEquals(offlineTopicStats.cursorDetails.get("c1").cursorBacklog, num);
    }

    @Test(timeOut = 20000)
    void testResetCursorAfterRecovery() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig conf = defaultConfig().setMaxEntriesPerLedger(10).setEnsembleSize(1)
                .setWriteQuorumSize(1).setAckQuorumSize(1);
        ManagedLedger ledger = factory.open("my_test_move_cursor_ledger", conf);
        ManagedCursor cursor = ledger.openCursor("trc1");
        Position p1 = ledger.addEntry("dummy-entry-1".getBytes());
        Position p2 = ledger.addEntry("dummy-entry-2".getBytes());
        Position p3 = ledger.addEntry("dummy-entry-3".getBytes());
        Position p4 = ledger.addEntry("dummy-entry-4".getBytes());

        cursor.markDelete(p3);

        @Cleanup("shutdown")
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory2.open("my_test_move_cursor_ledger", conf);
        cursor = ledger.openCursor("trc1");

        assertEquals(cursor.getMarkDeletedPosition(), p3);
        assertEquals(cursor.getReadPosition(), p4);
        assertEquals(cursor.getNumberOfEntriesInBacklog(false), 1);

        cursor.resetCursor(p2);
        assertEquals(cursor.getMarkDeletedPosition(), p1);
        assertEquals(cursor.getReadPosition(), p2);
        assertEquals(cursor.getNumberOfEntriesInBacklog(false), 3);
    }

    @Test(timeOut = 30000)
    public void managedLedgerClosed() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = defaultConfig();
        config.setEnsembleSize(2).setAckQuorumSize(2);
        ManagedLedgerImpl ledger1 = (ManagedLedgerImpl) factory.open("my_test_ledger" + testName, config);

        int num = 100;

        AtomicReference<ManagedLedgerException> res = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(num);

        for (int i = 0; i < num; i++) {
            ledger1.asyncAddEntry(("entry-" + i).getBytes(), new AddEntryCallback() {

                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    latch.countDown();
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    res.compareAndSet(null, exception);
                    latch.countDown();
                }
            }, null);

            if (i == 1) {
                ledger1.close();
            }
        }

        // Ensures all the callback must have been invoked
        latch.await();
        assertNotNull(res.get());
        assertEquals(res.get().getClass(), ManagedLedgerAlreadyClosedException.class);
    }

    @Test
    public void testChangeCrcType() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = defaultConfig();
        config.setEnsembleSize(2).setAckQuorumSize(2);
        config.setDigestType(DigestType.CRC32);
        ManagedLedger ledger = factory.open("my_test_ledger" + testName, config);
        ManagedCursor c1 = ledger.openCursor("c1");

        ledger.addEntry("entry-0".getBytes());
        ledger.addEntry("entry-1".getBytes());
        ledger.addEntry("entry-2".getBytes());

        ledger.close();

        config.setDigestType(DigestType.CRC32C);
        ledger = factory.open("my_test_ledger" + testName, config);
        c1 = ledger.openCursor("c1");

        ledger.addEntry("entry-3".getBytes());

        assertEquals(c1.getNumberOfEntries(), 4);
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 4);

        List<Entry> entries = c1.readEntries(4);
        assertEquals(entries.size(), 4);
        for (int i = 0; i < 4; i++) {
            assertEquals(new String(entries.get(i).getData()), "entry-" + i);
        }
    }

    @Test
    public void testPeriodicRollover() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);

        int rolloverTimeForCursorInSeconds = 5;

        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConf);
        ManagedLedgerConfig config = defaultConfig();
        config.setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1)
                .setLedgerRolloverTimeout(rolloverTimeForCursorInSeconds);
        ManagedLedger ledger = factory.open("my-ledger" + testName, config);
        ManagedCursor cursor = ledger.openCursor("c1");

        Position pos = ledger.addEntry("entry-0".getBytes());
        ledger.addEntry("entry-1".getBytes());

        List<Entry> entries = cursor.readEntries(2);
        assertEquals(2, entries.size());
        entries.forEach(Entry::release);

        ManagedCursorImpl cursorImpl = (ManagedCursorImpl) cursor;
        assertEquals(ManagedCursorImpl.State.NoLedger, cursorImpl.state);

        // this creates the ledger
        cursor.delete(pos);

        Awaitility.await().until(() -> cursorImpl.state == ManagedCursorImpl.State.Open);

        Thread.sleep(rolloverTimeForCursorInSeconds * 1000 + 1000);

        long currentLedgerId = cursorImpl.getCursorLedger();
        assertTrue(cursor.periodicRollover());
        Awaitility.await().until(() -> cursorImpl.getCursorLedger() != currentLedgerId);
    }

    @DataProvider(name = "booleans")
    public Object[][] booleans() {
        return new Object[][] {
                {true},
                {false},
        };
    }

    @Test(dataProvider = "booleans")
    public void testConfigPersistIndividualAckAsLongArray(boolean enable) throws Exception {
        final String mlName = "ml" + UUID.randomUUID().toString().replaceAll("-", "");
        final String cursorName = "c1";
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConf);
        final ManagedLedgerConfig config = defaultConfig()
                .setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1)
                .setMaxUnackedRangesToPersistInMetadataStore(1)
                .setPersistIndividualAckAsLongArray(enable);

        ManagedLedger ledger1 = factory.open(mlName, config);
        ManagedCursorImpl cursor1 = (ManagedCursorImpl) ledger1.openCursor(cursorName);

        // Write entries.
        int totalEntries = 100;
        List<Position> entries = new ArrayList<>();
        for (int i = 0; i < totalEntries; i++) {
            Position p = ledger1.addEntry("entry".getBytes());
            entries.add(p);
        }
        // Make ack holes and trigger a mark deletion.
        for (int i = totalEntries - 1; i >= 0; i--) {
            if (i % 2 == 0) {
                cursor1.delete(entries.get(i));
            }
        }
        cursor1.markDelete(entries.get(9));
        Awaitility.await().untilAsserted(() -> {
            assertEquals(cursor1.pendingMarkDeleteOps.size(), 0);
        });

        // Verify: the config affects.
        long cursorLedgerLac = cursor1.cursorLedger.getLastAddConfirmed();
        LedgerEntry ledgerEntry = cursor1.cursorLedger.readEntries(cursorLedgerLac, cursorLedgerLac).nextElement();
        PositionInfo positionInfo = new PositionInfo();
        positionInfo.parseFrom(ledgerEntry.getEntry());
        if (enable) {
            assertNotEquals(positionInfo.getIndividualDeletedMessageRangesCount(), 0);
        } else {
            assertEquals(positionInfo.getIndividualDeletedMessageRangesCount(), 0);
        }

        // cleanup
        ledger1.close();
        factory.shutdown();
    }

    /**
     * Terminating a managed ledger closes the current BookKeeper ledger, and that close errors out the adds which are
     * still in flight. These adds can never succeed, so they must be failed rather than left pending forever: a caller
     * that tracks its in-flight writes (like the broker's PersistentTopic) would otherwise never see them complete.
     */
    @Test
    public void testTerminateFailsInFlightAdds() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);

        ManagedLedgerConfig config = defaultConfig().setEnsembleSize(1).setWriteQuorumSize(1)
                .setAckQuorumSize(1);
        // A terminated ledger cannot be written to again: use a name that a retry of this test would not reuse
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger" + UUID.randomUUID(), config);
        Position lastPersisted = ledger.addEntry("entry-0".getBytes());

        // Suspend the bookie that stores the current ledger, so that the next add stays in flight
        BookieId bookie = ledger.currentLedger.getLedgerMetadata().getAllEnsembles().firstEntry().getValue().get(0);
        CountDownLatch resumeBookie = new CountDownLatch(1);
        sleepBookie(bookie, resumeBookie);
        try {
            CompletableFuture<ManagedLedgerException> inFlightAddFailure = new CompletableFuture<>();
            ledger.asyncAddEntry("entry-1".getBytes(), new AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    inFlightAddFailure.completeExceptionally(
                            new AssertionError("The in-flight add should not succeed: " + position));
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    inFlightAddFailure.complete(exception);
                }
            }, null);
            Awaitility.await().until(() -> !ledger.pendingAddEntries.isEmpty());

            assertEquals(ledger.terminate(), lastPersisted);

            assertTrue(inFlightAddFailure.get(10, TimeUnit.SECONDS) instanceof ManagedLedgerTerminatedException);
            assertTrue(ledger.pendingAddEntries.isEmpty());
        } finally {
            resumeBookie.countDown();
        }
    }
}
