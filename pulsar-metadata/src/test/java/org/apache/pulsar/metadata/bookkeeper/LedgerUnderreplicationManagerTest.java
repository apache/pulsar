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
package org.apache.pulsar.metadata.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.LayoutManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.UnderreplicatedLedger;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.net.DNS;
import org.apache.bookkeeper.proto.UnderreplicatedLedgerFormat;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.migration.MigrationPhase;
import org.apache.pulsar.common.migration.MigrationState;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.BaseMetadataStoreTest;
import org.apache.pulsar.metadata.TestZKServer;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.pulsar.metadata.impl.DualMetadataStore;
import org.apache.pulsar.metadata.impl.MetadataStoreFactoryImpl;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.metadata.impl.batching.MetadataOp;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test the zookeeper implementation of the ledger replication manager.
 */
@CustomLog
public class LedgerUnderreplicationManagerTest extends BaseMetadataStoreTest {

    private Future<Long> getLedgerToReplicate(LedgerUnderreplicationManager m) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.info("Starting thread checking for ledgers");
                long l = m.getLedgerToRereplicate();
                log.info().attr("ledgerId", Long.toHexString(l)).log("Get ledger id");
                return l;
            } catch (Exception e) {
                log.error().exception(e).log("Error getting ledger id");
                return -1L;
            }
        }, executor);
    }

    private MetadataStoreExtended store;
    private LayoutManager layoutManager;
    private LedgerManagerFactory lmf;
    private LedgerUnderreplicationManager lum;

    private String basePath;
    private String ledgersRoot;
    private String urLedgerPath;
    private ExecutorService executor;

    @SuppressWarnings("deprecation")
    private void methodSetup(Supplier<String> urlSupplier) throws Exception {
        methodSetup(MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build()));
    }

    @SuppressWarnings("deprecation")
    private void methodSetup(MetadataStoreExtended metadataStore) throws Exception {
        this.executor = Executors.newSingleThreadExecutor();
        ledgersRoot = "/ledgers-" + UUID.randomUUID();
        this.store = metadataStore;
        this.layoutManager = new PulsarLayoutManager(store, ledgersRoot);
        this.lmf = new PulsarLedgerManagerFactory();

        ClientConfiguration conf = new ClientConfiguration();
        conf.setZkLedgersRootPath(ledgersRoot);
        this.lmf.initialize(conf, layoutManager, 1);
        this.lum = lmf.newLedgerUnderreplicationManager();

        basePath = ledgersRoot + '/'
                + BookKeeperConstants.UNDER_REPLICATION_NODE;
        urLedgerPath = basePath
                + BookKeeperConstants.DEFAULT_ZK_LEDGERS_ROOT_PATH;
    }

    @AfterMethod(alwaysRun = true)
    public final void methodCleanup() throws Exception {
        if (lum != null) {
            lum.close();
        }
        if (lmf != null) {
            lmf.close();
        }
        if (store != null) {
            store.close();
        }
        if (executor != null) {
            try {
                executor.shutdownNow();
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            executor = null;
        }
    }

    @DataProvider(name = "lockCleanup")
    public Object[][] lockCleanup() {
        return new Object[][]{{false, false}, {false, true}, {true, false}, {true, true}};
    }

    @Test(timeOut = 60000, dataProvider = "lockCleanup")
    public void testLockCleanupAfterSessionExpiration(boolean explicit, boolean close) throws Exception {
        methodSetup(zks::getConnectionString);
        long ledgerId = 123L;
        if (explicit) {
            lum.acquireUnderreplicatedLedger(ledgerId);
        } else {
            lum.markLedgerUnderreplicated(ledgerId, "bookie:3181");
            assertThat(lum.pollLedgerToRereplicate()).isEqualTo(ledgerId);
        }

        CountDownLatch reestablished = new CountDownLatch(1);
        store.registerSessionListener(event -> {
            if (event == SessionEvent.SessionReestablished) {
                reestablished.countDown();
            }
        });
        ZKMetadataStore zkStore = (ZKMetadataStore) ((DualMetadataStore) store).getSourceStore();
        zks.expireSession(zkStore.getZkSessionId());
        assertThat(reestablished.await(20, TimeUnit.SECONDS)).isTrue();

        try (var other = lmf.newLedgerUnderreplicationManager()) {
            other.acquireUnderreplicatedLedger(ledgerId);
            if (close) {
                lum.close();
            } else {
                lum.releaseUnderreplicatedLedger(ledgerId);
            }
            assertThat(other.isLedgerBeingReplicated(ledgerId))
                    .as("Cleanup from the expired session must preserve the new owner's lock").isTrue();
            other.releaseUnderreplicatedLedger(ledgerId);
            lum.acquireUnderreplicatedLedger(ledgerId);
            lum.releaseUnderreplicatedLedger(ledgerId);
            assertThat(other.isLedgerBeingReplicated(ledgerId)).isFalse();
        }
    }

    @Test(timeOut = 60000, dataProvider = "lockCleanup")
    public void testLockCleanupAfterAmbiguousDelete(boolean interrupted, boolean close) throws Exception {
        methodSetup(zks::getConnectionString);
        long ledgerId = 123L;
        lum.acquireUnderreplicatedLedger(ledgerId);
        String lockPath = PulsarLedgerUnderreplicationManager.getUrLedgerLockPath(
                PulsarLedgerUnderreplicationManager.getUrLockPath(ledgersRoot), ledgerId);
        CountDownLatch callbackBlocked = new CountDownLatch(1);
        CountDownLatch resumeCallbacks = new CountDownLatch(1);

        // ZooKeeper sends the watch event before acknowledging the delete to its owner.
        // Pause that real notification so the already applied delete's result remains pending.
        store.registerListener(notification -> {
            if (notification.getPath().equals(lockPath)
                    && notification.getType() == NotificationType.Deleted) {
                callbackBlocked.countDown();
                try {
                    assertThat(resumeCallbacks.await(50, TimeUnit.SECONDS)).isTrue();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        try (MetadataStoreExtended otherStore = MetadataStoreExtended.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().build());
             var other = new PulsarLedgerUnderreplicationManager(new ClientConfiguration(), otherStore, ledgersRoot)) {
            AtomicReference<Thread> cleanupThread = new AtomicReference<>();
            Future<UnavailableException> firstCleanup = executor.submit(() -> {
                cleanupThread.set(Thread.currentThread());
                try {
                    if (close) {
                        lum.close();
                    } else {
                        lum.releaseUnderreplicatedLedger(ledgerId);
                    }
                    throw new AssertionError("Cleanup must fail while its delete acknowledgement is blocked");
                } catch (UnavailableException error) {
                    assertThat(Thread.currentThread().isInterrupted()).isEqualTo(interrupted);
                    return error;
                } finally {
                    Thread.interrupted();
                }
            });
            Future<?> repeatedCleanup;
            try {
                assertThat(callbackBlocked.await(10, TimeUnit.SECONDS)).isTrue();
                if (interrupted) {
                    cleanupThread.get().interrupt();
                }
                assertThat(firstCleanup.get(40, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(interrupted ? InterruptedException.class : TimeoutException.class);

                Awaitility.await().untilAsserted(() -> assertThat(otherStore.get(lockPath).join()).isEmpty());
                other.acquireUnderreplicatedLedger(ledgerId);

                AtomicReference<Thread> retryThread = new AtomicReference<>();
                repeatedCleanup = executor.submit(() -> {
                    retryThread.set(Thread.currentThread());
                    lum.releaseUnderreplicatedLedger(ledgerId);
                    lum.close();
                    return null;
                });
                // The second cleanup is waiting while the first delete's acknowledgement is still blocked.
                Awaitility.await().until(() -> retryThread.get() != null
                        && retryThread.get().getState() == Thread.State.TIMED_WAITING);
            } finally {
                resumeCallbacks.countDown();
            }
            repeatedCleanup.get(10, TimeUnit.SECONDS);

            // Both cleanup entry points must reuse the first deletion, including across calls.
            lum.releaseUnderreplicatedLedger(ledgerId);
            lum.close();
            assertThat(other.isLedgerBeingReplicated(ledgerId))
                    .as("Retrying an ambiguous delete must preserve the new owner's lock").isTrue();
        } finally {
            resumeCallbacks.countDown();
        }
    }

    @DataProvider(name = "cleanupMethods")
    public Object[][] cleanupMethods() {
        return new Object[][]{{false}, {true}};
    }

    @Test(timeOut = 60000, dataProvider = "cleanupMethods")
    public void testLockCleanupAfterSameSessionRecovery(boolean close) throws Exception {
        methodSetup(MetadataStoreExtended.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().sessionTimeoutMillis(12000).build()));
        long ledgerId = 123L;
        lum.acquireUnderreplicatedLedger(ledgerId);
        ZKMetadataStore zkStore = (ZKMetadataStore) ((DualMetadataStore) store).getSourceStore();
        long sessionId = zkStore.getZkSessionId();
        CountDownLatch lost = new CountDownLatch(1);
        CountDownLatch reestablished = new CountDownLatch(1);
        store.registerSessionListener(event -> {
            if (event == SessionEvent.SessionLost) {
                lost.countDown();
            } else if (event == SessionEvent.SessionReestablished) {
                reestablished.countDown();
            }
        });

        zks.stop();
        try {
            // The watcher reports loss at 5/6 of the negotiated timeout, before actual expiration.
            assertThat(lost.await(20, TimeUnit.SECONDS)).isTrue();
        } finally {
            zks.start();
        }
        assertThat(reestablished.await(20, TimeUnit.SECONDS)).isTrue();
        assertThat(zkStore.getZkSessionId()).as("The original session must survive the outage").isEqualTo(sessionId);
        assertThat(lum.isLedgerBeingReplicated(ledgerId)).isTrue();
        if (close) {
            lum.close();
        } else {
            lum.releaseUnderreplicatedLedger(ledgerId);
        }
        assertThat(lum.isLedgerBeingReplicated(ledgerId)).as("The surviving lock must remain releasable").isFalse();
    }

    @DataProvider(name = "migrationCleanup")
    public Object[][] migrationCleanup() {
        List<Object[]> cases = new ArrayList<>(List.of(new Object[]{false, false, false, false},
                new Object[]{false, true, false, false}, new Object[]{false, false, true, false},
                new Object[]{false, true, true, false}, new Object[]{false, false, false, true},
                new Object[]{false, true, false, true}));
        // Honor the existing provider selection so scoped runs can avoid the Oxia container.
        if (isOxiaEnabled()) {
            cases.addAll(List.of(new Object[]{true, false, false, false}, new Object[]{true, true, false, false},
                    new Object[]{true, false, true, false}, new Object[]{true, true, true, false},
                    new Object[]{true, false, false, true}, new Object[]{true, true, false, true}));
        }
        return cases.toArray(Object[][]::new);
    }

    private boolean isOxiaEnabled() {
        for (Object[] implementation : implementations()) {
            if ("Oxia".equals(implementation[0])) {
                return true;
            }
        }
        return false;
    }

    @DataProvider(name = "failedMigrationCleanup")
    public Object[][] failedMigrationCleanup() {
        List<Object[]> cases = new ArrayList<>(List.of(new Object[]{false, false}, new Object[]{false, true}));
        if (isOxiaEnabled()) {
            cases.addAll(List.of(new Object[]{true, false}, new Object[]{true, true}));
        }
        return cases.toArray(Object[][]::new);
    }

    @DataProvider(name = "failedMigrationExpiredSource")
    public Object[][] failedMigrationExpiredSource() {
        List<Object[]> cases = new ArrayList<>();
        for (Object[] entry : failedMigrationCleanup()) {
            cases.add(new Object[]{entry[0], entry[1], false});
            cases.add(new Object[]{entry[0], entry[1], true});
        }
        return cases.toArray(Object[][]::new);
    }

    private void cleanupLock(LedgerUnderreplicationManager manager, long ledgerId, boolean close)
            throws UnavailableException {
        if (close) {
            manager.close();
        } else {
            manager.releaseUnderreplicatedLedger(ledgerId);
        }
    }

    private String ledgerLockPath(long ledgerId) {
        return PulsarLedgerUnderreplicationManager.getUrLedgerLockPath(
                PulsarLedgerUnderreplicationManager.getUrLockPath(ledgersRoot), ledgerId);
    }

    private void setMigrationPhase(MetadataStoreExtended source, MigrationPhase phase, String targetUrl)
            throws Exception {
        source.put(MigrationState.MIGRATION_FLAG_PATH, ObjectMapperFactory.getMapper().writer().writeValueAsBytes(
                new MigrationState(phase, targetUrl)), Optional.empty()).join();
    }

    private void prepareLockMigration(MetadataStoreExtended source, MetadataStoreExtended target, String targetUrl,
                                      String lockPath, BlockingQueue<SessionEvent> sessionEvents) throws Exception {
        setMigrationPhase(source, MigrationPhase.PREPARATION, targetUrl);
        Awaitility.await().untilAsserted(() -> {
            assertThat(target.get(lockPath).join()).isPresent();
            assertThat(source.getChildren(MigrationState.PARTICIPANTS_PATH).join()).isEmpty();
        });
        assertThat(sessionEvents.poll(10, TimeUnit.SECONDS)).isEqualTo(SessionEvent.SessionLost);
    }

    private void failMigration(MetadataStoreExtended source, String targetUrl,
                               BlockingQueue<SessionEvent> sessionEvents) throws Exception {
        setMigrationPhase(source, MigrationPhase.FAILED, targetUrl);
        assertThat(sessionEvents.poll(10, TimeUnit.SECONDS)).isEqualTo(SessionEvent.SessionReestablished);
    }

    private void resetMigration(MetadataStoreExtended source, DualMetadataStore dualStore) throws Exception {
        source.delete(MigrationState.MIGRATION_FLAG_PATH, Optional.empty()).join();
        Awaitility.await().until(() -> dualStore.getMigrationPhase() == MigrationPhase.NOT_STARTED);
    }

    private void expireSourceSession(ZKMetadataStore source) throws Exception {
        CountDownLatch reestablished = new CountDownLatch(1);
        source.registerSessionListener(event -> {
            if (event == SessionEvent.SessionReestablished) {
                reestablished.countDown();
            }
        });
        zks.expireSession(source.getZkSessionId());
        assertThat(reestablished.await(20, TimeUnit.SECONDS)).isTrue();
    }

    @Test(timeOut = 60000, dataProvider = "cleanupMethods")
    public void testFailedMigrationDoesNotCopyLaterAcquisition(boolean close) throws Exception {
        AtomicReference<String> delayedPath = new AtomicReference<>();
        AtomicBoolean delayFirstRead = new AtomicBoolean(true);
        CountDownLatch readReady = new CountDownLatch(1);
        CompletableFuture<Void> resumeRead = new CompletableFuture<>();
        ZKMetadataStore source = new ZKMetadataStore(zks.getConnectionString(),
                MetadataStoreConfig.builder().build(), true) {
            @Override
            protected void batchOperation(List<MetadataOp> operations) {
                List<MetadataOp> immediate = new ArrayList<>();
                for (MetadataOp operation : operations) {
                    if (operation.getType() == MetadataOp.Type.GET && operation.getPath().equals(delayedPath.get())
                            && delayFirstRead.compareAndSet(true, false)) {
                        readReady.countDown();
                        resumeRead.thenRun(() -> super.batchOperation(List.of(operation)));
                    } else {
                        immediate.add(operation);
                    }
                }
                if (!immediate.isEmpty()) {
                    super.batchOperation(immediate);
                }
            }
        };
        DualMetadataStore dualStore = new DualMetadataStore(source, MetadataStoreConfig.builder().build());
        methodSetup(dualStore);
        long ledgerId = 123L;
        lum.acquireUnderreplicatedLedger(ledgerId);
        String lockPath = ledgerLockPath(ledgerId);
        GetResult acquired = source.get(lockPath).join().orElseThrow();
        BlockingQueue<SessionEvent> sessionEvents = new LinkedBlockingQueue<>();
        store.registerSessionListener(sessionEvents::add);
        try (TestZKServer targetServer = new TestZKServer()) {
            String targetUrl = targetServer.getConnectionString();
            try (MetadataStoreExtended target = MetadataStoreExtended.create(targetUrl,
                    MetadataStoreConfig.builder().build())) {
                delayedPath.set(lockPath);
                setMigrationPhase(source, MigrationPhase.PREPARATION, targetUrl);
                assertThat(readReady.await(10, TimeUnit.SECONDS)).isTrue();
                assertThat(sessionEvents.poll(10, TimeUnit.SECONDS)).isEqualTo(SessionEvent.SessionLost);
                setMigrationPhase(source, MigrationPhase.FAILED, targetUrl);
                Awaitility.await().until(() -> dualStore.getMigrationPhase() == MigrationPhase.FAILED);
                source.delete(lockPath, Optional.of(acquired.getStat().getVersion())).join();
                try (var other = lmf.newLedgerUnderreplicationManager()) {
                    other.acquireUnderreplicatedLedger(ledgerId);
                    // Resume the actual source read after FAILED and acquisition of a different lock incarnation.
                    resumeRead.complete(null);
                    assertThat(sessionEvents.poll(10, TimeUnit.SECONDS)).isEqualTo(SessionEvent.SessionReestablished);
                    assertThat(target.get(lockPath).join()).as("The old preparation must not copy the new lock")
                            .isEmpty();
                    targetServer.stop();
                    Future<?> cleanup = executor.submit(() -> {
                        cleanupLock(other, ledgerId, close);
                        return null;
                    });
                    try {
                        cleanup.get(3, TimeUnit.SECONDS);
                        assertThat(source.get(lockPath).join()).isEmpty();
                    } finally {
                        targetServer.start();
                        cleanup.get(20, TimeUnit.SECONDS);
                    }
                }
            } finally {
                resumeRead.complete(null);
                resetMigration(source, dualStore);
                try {
                    lum.close();
                } finally {
                    store.close();
                }
            }
        }
    }

    @Test(timeOut = 60000, dataProvider = "lockCleanup")
    public void testNewLockCleanupWithFailedMigrationTarget(boolean samePath, boolean close) throws Exception {
        methodSetup(zks::getConnectionString);
        long originalLedgerId = 123L;
        lum.acquireUnderreplicatedLedger(originalLedgerId);
        String originalPath = ledgerLockPath(originalLedgerId);
        DualMetadataStore dualStore = (DualMetadataStore) store;
        ZKMetadataStore source = (ZKMetadataStore) dualStore.getSourceStore();
        BlockingQueue<SessionEvent> sessionEvents = new LinkedBlockingQueue<>();
        store.registerSessionListener(sessionEvents::add);
        try (TestZKServer targetServer = new TestZKServer()) {
            String targetUrl = targetServer.getConnectionString();
            try (MetadataStoreExtended target = MetadataStoreExtended.create(targetUrl,
                    MetadataStoreConfig.builder().build())) {
                prepareLockMigration(source, target, targetUrl, originalPath, sessionEvents);
                failMigration(source, targetUrl, sessionEvents);
                long ledgerId = samePath ? originalLedgerId : 124L;
                if (samePath) {
                    expireSourceSession(source);
                }
                try (var other = lmf.newLedgerUnderreplicationManager()) {
                    other.acquireUnderreplicatedLedger(ledgerId);
                    String lockPath = ledgerLockPath(ledgerId);
                    assertThat(source.get(lockPath).join().orElseThrow().getStat().isCreatedBySelf()).isTrue();
                    if (samePath) {
                        assertThat(target.get(lockPath).join().orElseThrow().getValue())
                                .isNotEqualTo(source.get(lockPath).join().orElseThrow().getValue());
                    } else {
                        assertThat(target.get(lockPath).join()).isEmpty();
                    }
                    // A real target outage must not delay deletion of an acquisition never copied there.
                    targetServer.stop();
                    Future<?> cleanup = executor.submit(() -> {
                        cleanupLock(other, ledgerId, close);
                        return null;
                    });
                    try {
                        cleanup.get(3, TimeUnit.SECONDS);
                        assertThat(source.get(lockPath).join()).isEmpty();
                    } finally {
                        targetServer.start();
                        cleanup.get(20, TimeUnit.SECONDS);
                    }
                }
            } finally {
                resetMigration(source, dualStore);
                try {
                    lum.close();
                } finally {
                    store.close();
                }
            }
        }
    }

    @Test(timeOut = 60000, dataProvider = "cleanupMethods")
    public void testLockCleanupAfterStartingWithFailedMigration(boolean close) throws Exception {
        ZKMetadataStore source = new ZKMetadataStore(zks.getConnectionString(),
                MetadataStoreConfig.builder().build(), true);
        setMigrationPhase(source, MigrationPhase.FAILED, "memory:" + UUID.randomUUID());
        DualMetadataStore dualStore = new DualMetadataStore(source, MetadataStoreConfig.builder().build());
        methodSetup(dualStore);
        long ledgerId = 123L;
        lum.acquireUnderreplicatedLedger(ledgerId);
        String lockPath = ledgerLockPath(ledgerId);
        CountDownLatch reestablished = new CountDownLatch(1);
        source.registerSessionListener(event -> {
            if (event == SessionEvent.SessionReestablished) {
                reestablished.countDown();
            }
        });
        try {
            zks.expireSession(source.getZkSessionId());
            assertThat(reestablished.await(20, TimeUnit.SECONDS)).isTrue();
            assertThat(source.get(lockPath).join()).isEmpty();
            cleanupLock(lum, ledgerId, close);
            lum.close();
        } finally {
            resetMigration(source, dualStore);
        }
    }

    @SuppressWarnings("deprecation")
    @Test(timeOut = 60000, dataProvider = "cleanupMethods")
    public void testFailedMigrationCleanupPreservesForeignSourceOwnership(boolean close) throws Exception {
        AtomicReference<String> participantPath = new AtomicReference<>();
        AtomicBoolean retryPreparation = new AtomicBoolean();
        CountDownLatch originalPreparationFinished = new CountDownLatch(1);
        ZKMetadataStore source = new ZKMetadataStore(zks.getConnectionString(),
                MetadataStoreConfig.builder().build(), true) {
            @Override
            protected void batchOperation(List<MetadataOp> operations) {
                if (retryPreparation.get() && operations.stream().anyMatch(operation ->
                        operation.getType() == MetadataOp.Type.DELETE
                                && operation.getPath().equals(participantPath.get()))) {
                    // The participant delete is submitted only after all copy operations complete.
                    originalPreparationFinished.countDown();
                }
                super.batchOperation(operations);
            }
        };
        DualMetadataStore dualStore = new DualMetadataStore(source, MetadataStoreConfig.builder().build());
        methodSetup(dualStore);
        participantPath.set(MigrationState.PARTICIPANTS_PATH + '/'
                + source.getChildren(MigrationState.PARTICIPANTS_PATH).join().get(0));
        long ledgerId = 123L;
        lum.acquireUnderreplicatedLedger(ledgerId);
        String lockPath = ledgerLockPath(ledgerId);
        BlockingQueue<SessionEvent> sessionEvents = new LinkedBlockingQueue<>();
        store.registerSessionListener(sessionEvents::add);
        CompletableFuture<Void> resumeForeignRead = new CompletableFuture<>();
        try (TestZKServer targetServer = new TestZKServer()) {
            String targetUrl = targetServer.getConnectionString();
            try (MetadataStoreExtended target = MetadataStoreExtended.create(targetUrl,
                    MetadataStoreConfig.builder().build())) {
                prepareLockMigration(source, target, targetUrl, lockPath, sessionEvents);
                failMigration(source, targetUrl, sessionEvents);
                expireSourceSession(source);
                AtomicBoolean delayForeignRead = new AtomicBoolean();
                CountDownLatch foreignReadReady = new CountDownLatch(1);
                ZKMetadataStore foreignSource = new ZKMetadataStore(zks.getConnectionString(),
                        MetadataStoreConfig.builder().build(), true) {
                    @Override
                    protected void batchOperation(List<MetadataOp> operations) {
                        List<MetadataOp> immediate = new ArrayList<>();
                        for (MetadataOp operation : operations) {
                            if (operation.getType() == MetadataOp.Type.GET && operation.getPath().equals(lockPath)
                                    && delayForeignRead.compareAndSet(true, false)) {
                                foreignReadReady.countDown();
                                resumeForeignRead.thenRun(() -> super.batchOperation(List.of(operation)));
                            } else {
                                immediate.add(operation);
                            }
                        }
                        if (!immediate.isEmpty()) {
                            super.batchOperation(immediate);
                        }
                    }
                };
                try (DualMetadataStore foreignStore = new DualMetadataStore(foreignSource,
                        MetadataStoreConfig.builder().build())) {
                    @Cleanup
                    PulsarLedgerManagerFactory foreignFactory = new PulsarLedgerManagerFactory();
                    ClientConfiguration conf = new ClientConfiguration();
                    conf.setZkLedgersRootPath(ledgersRoot);
                    foreignFactory.initialize(conf, new PulsarLayoutManager(foreignStore, ledgersRoot), 1);
                    try (var other = foreignFactory.newLedgerUnderreplicationManager()) {
                        other.acquireUnderreplicatedLedger(ledgerId);
                        assertThat(source.get(lockPath).join().orElseThrow().getStat().isCreatedBySelf()).isFalse();
                        cleanupLock(lum, ledgerId, close);
                        assertThat(target.get(lockPath).join()).isEmpty();
                        retryPreparation.set(true);
                        delayForeignRead.set(true);
                        setMigrationPhase(source, MigrationPhase.PREPARATION, targetUrl);
                        try {
                            assertThat(foreignReadReady.await(10, TimeUnit.SECONDS)).isTrue();
                            assertThat(originalPreparationFinished.await(10, TimeUnit.SECONDS)).isTrue();
                            assertThat(target.get(lockPath).join())
                                    .as("A stale source path must not copy another source session's acquisition")
                                    .isEmpty();
                        } finally {
                            resumeForeignRead.complete(null);
                        }
                        Awaitility.await().untilAsserted(() -> {
                            assertThat(target.get(lockPath).join()).isPresent();
                            assertThat(source.getChildren(MigrationState.PARTICIPANTS_PATH).join()).isEmpty();
                        });
                        setMigrationPhase(source, MigrationPhase.COMPLETED, targetUrl);
                        Awaitility.await().until(() -> foreignStore.getMigrationPhase() == MigrationPhase.COMPLETED);
                        cleanupLock(other, ledgerId, close);
                        assertThat(target.get(lockPath).join()).isEmpty();
                    }
                }
            } finally {
                resumeForeignRead.complete(null);
                resetMigration(source, dualStore);
                try {
                    lum.close();
                } finally {
                    store.close();
                }
            }
        }
    }

    @Test(timeOut = 60000, dataProvider = "failedMigrationExpiredSource")
    public void testFailedMigrationCleanupAfterSourceExpiration(boolean oxia, boolean close, boolean replaceSource)
            throws Exception {
        methodSetup(zks::getConnectionString);
        long ledgerId = 123L;
        lum.acquireUnderreplicatedLedger(ledgerId);
        String lockPath = ledgerLockPath(ledgerId);
        DualMetadataStore dualStore = (DualMetadataStore) store;
        ZKMetadataStore source = (ZKMetadataStore) dualStore.getSourceStore();
        String targetUrl = oxia ? "oxia://" + getOxiaServerConnectString() : "memory:" + UUID.randomUUID();
        BlockingQueue<SessionEvent> sessionEvents = new LinkedBlockingQueue<>();
        store.registerSessionListener(sessionEvents::add);
        try (MetadataStoreExtended target = MetadataStoreExtended.create(targetUrl,
                MetadataStoreConfig.builder().build())) {
            prepareLockMigration(source, target, targetUrl, lockPath, sessionEvents);
            failMigration(source, targetUrl, sessionEvents);
            expireSourceSession(source);
            assertThat(source.get(lockPath).join()).isEmpty();
            assertThat(target.get(lockPath).join()).isPresent();
            try (var other = replaceSource ? lmf.newLedgerUnderreplicationManager() : null) {
                GetResult replacement = null;
                if (replaceSource) {
                    other.acquireUnderreplicatedLedger(ledgerId);
                    replacement = source.get(lockPath).join().orElseThrow();
                }
                cleanupLock(lum, ledgerId, close);
                assertThat(target.get(lockPath).join()).isEmpty();
                if (replaceSource) {
                    assertThat(source.get(lockPath).join()).isPresent().get()
                            .extracting(GetResult::getValue).isEqualTo(replacement.getValue());
                    setMigrationPhase(source, MigrationPhase.PREPARATION, targetUrl);
                    byte[] replacementData = replacement.getValue();
                    Awaitility.await().untilAsserted(() -> assertThat(target.get(lockPath).join()).isPresent().get()
                            .extracting(GetResult::getValue).isEqualTo(replacementData));
                    setMigrationPhase(source, MigrationPhase.COMPLETED, targetUrl);
                    Awaitility.await().until(() -> dualStore.getMigrationPhase() == MigrationPhase.COMPLETED);
                    cleanupLock(other, ledgerId, close);
                    assertThat(target.get(lockPath).join()).isEmpty();
                }
            }
        } finally {
            resetMigration(source, dualStore);
        }
    }

    @Test(timeOut = 60000, dataProvider = "failedMigrationCleanup")
    public void testLockCleanupDuringFailedMetadataMigration(boolean oxia, boolean close) throws Exception {
        methodSetup(zks::getConnectionString);
        long ledgerId = 123L;
        lum.acquireUnderreplicatedLedger(ledgerId);
        String lockPath = ledgerLockPath(ledgerId);
        MetadataStoreExtended source = ((DualMetadataStore) store).getSourceStore();
        String targetUrl = oxia ? "oxia://" + getOxiaServerConnectString() : "memory:" + UUID.randomUUID();
        BlockingQueue<SessionEvent> sessionEvents = new LinkedBlockingQueue<>();
        store.registerSessionListener(sessionEvents::add);
        try (MetadataStoreExtended target = MetadataStoreExtended.create(targetUrl,
                MetadataStoreConfig.builder().build())) {
            prepareLockMigration(source, target, targetUrl, lockPath, sessionEvents);
            failMigration(source, targetUrl, sessionEvents);
            assertThat(source.get(lockPath).join().orElseThrow().getStat().isCreatedBySelf()).isTrue();

            if (close) {
                lum.close();
            } else {
                lum.releaseUnderreplicatedLedger(ledgerId);
            }
            assertThat(source.get(lockPath).join()).isEmpty();
            assertThat(target.get(lockPath).join())
                    .as("Cleanup during FAILED must also release the owned target-store copy").isEmpty();

            setMigrationPhase(source, MigrationPhase.PREPARATION, targetUrl);
            assertThat(sessionEvents.poll(10, TimeUnit.SECONDS)).isEqualTo(SessionEvent.SessionLost);
            setMigrationPhase(source, MigrationPhase.COMPLETED, targetUrl);
            assertThat(sessionEvents.poll(10, TimeUnit.SECONDS)).isEqualTo(SessionEvent.SessionReestablished);
            assertThat(lum.isLedgerBeingReplicated(ledgerId))
                    .as("A migration retry must not resurrect an already released acquisition").isFalse();
        } finally {
            source.delete(MigrationState.MIGRATION_FLAG_PATH, Optional.empty()).join();
        }
    }

    @Test(timeOut = 60000, dataProvider = "cleanupMethods")
    public void testFailedMigrationCleanupPreservesTargetReplacement(boolean close) throws Exception {
        methodSetup(zks::getConnectionString);
        long ledgerId = 123L;
        lum.acquireUnderreplicatedLedger(ledgerId);
        String lockPath = ledgerLockPath(ledgerId);
        MetadataStoreExtended source = ((DualMetadataStore) store).getSourceStore();
        BlockingQueue<SessionEvent> sessionEvents = new LinkedBlockingQueue<>();
        store.registerSessionListener(sessionEvents::add);
        try (TestZKServer targetServer = new TestZKServer()) {
            String targetUrl = targetServer.getConnectionString();
            try (MetadataStoreExtended target = MetadataStoreExtended.create(targetUrl,
                    MetadataStoreConfig.builder().build())) {
                prepareLockMigration(source, target, targetUrl, lockPath, sessionEvents);
                failMigration(source, targetUrl, sessionEvents);

                GetResult copied = target.get(lockPath).join().orElseThrow();
                target.delete(lockPath, Optional.of(copied.getStat().getVersion())).join();
                target.put(lockPath, copied.getValue(), Optional.of(-1L), EnumSet.of(CreateOption.Ephemeral)).join();
                GetResult replacement = target.get(lockPath).join().orElseThrow();
                assertThat(replacement.getStat().isCreatedBySelf()).isTrue();
                assertThat(replacement.getStat().getVersion()).isEqualTo(copied.getStat().getVersion());

                if (close) {
                    lum.close();
                } else {
                    lum.releaseUnderreplicatedLedger(ledgerId);
                }
                assertThat(source.get(lockPath).join()).isEmpty();
                assertThat(target.get(lockPath).join()).as("A matching value does not imply target session ownership")
                        .isPresent().get().extracting(GetResult::getValue).isEqualTo(replacement.getValue());
            } finally {
                source.delete(MigrationState.MIGRATION_FLAG_PATH, Optional.empty()).join();
                try {
                    lum.close();
                } finally {
                    store.close();
                }
            }
        }
    }

    @Test(timeOut = 60000, dataProvider = "failedMigrationCleanup")
    public void testLockCleanupRetriesSourceFailureAfterFailedMigration(boolean oxia, boolean close) throws Exception {
        AtomicReference<String> delayedPath = new AtomicReference<>();
        AtomicBoolean delayFirstDeletion = new AtomicBoolean(true);
        CountDownLatch deletionReady = new CountDownLatch(1);
        CompletableFuture<Void> resumeDeletion = new CompletableFuture<>();
        // Delay the real source delete after target cleanup; all reads and writes still use the real stores.
        ZKMetadataStore source = new ZKMetadataStore(zks.getConnectionString(),
                MetadataStoreConfig.builder().build(), true) {
            @Override
            protected void batchOperation(List<MetadataOp> operations) {
                List<MetadataOp> immediate = new ArrayList<>();
                for (MetadataOp operation : operations) {
                    if (operation.getType() == MetadataOp.Type.DELETE && operation.getPath().equals(delayedPath.get())
                            && delayFirstDeletion.compareAndSet(true, false)) {
                        deletionReady.countDown();
                        resumeDeletion.thenRun(() -> super.batchOperation(List.of(operation)));
                    } else {
                        immediate.add(operation);
                    }
                }
                if (!immediate.isEmpty()) {
                    super.batchOperation(immediate);
                }
            }
        };
        methodSetup(new DualMetadataStore(source, MetadataStoreConfig.builder().build()));
        long ledgerId = 123L;
        lum.acquireUnderreplicatedLedger(ledgerId);
        String lockPath = ledgerLockPath(ledgerId);
        GetResult acquired = source.get(lockPath).join().orElseThrow();
        String targetUrl = oxia ? "oxia://" + getOxiaServerConnectString() : "memory:" + UUID.randomUUID();
        BlockingQueue<SessionEvent> sessionEvents = new LinkedBlockingQueue<>();
        store.registerSessionListener(sessionEvents::add);
        try (MetadataStoreExtended target = MetadataStoreExtended.create(targetUrl,
                MetadataStoreConfig.builder().build())) {
            prepareLockMigration(source, target, targetUrl, lockPath, sessionEvents);
            failMigration(source, targetUrl, sessionEvents);
            delayedPath.set(lockPath);
            Future<UnavailableException> cleanup = executor.submit(() -> {
                try {
                    if (close) {
                        lum.close();
                    } else {
                        lum.releaseUnderreplicatedLedger(ledgerId);
                    }
                    return null;
                } catch (UnavailableException error) {
                    return error;
                }
            });
            try {
                assertThat(deletionReady.await(10, TimeUnit.SECONDS)).isTrue();
                assertThat(target.get(lockPath).join()).as("Target cleanup precedes the source delete").isEmpty();
                source.put(lockPath, acquired.getValue(), Optional.of(acquired.getStat().getVersion()),
                        EnumSet.of(CreateOption.Ephemeral)).join();
            } finally {
                resumeDeletion.complete(null);
            }
            UnavailableException failure = cleanup.get(10, TimeUnit.SECONDS);
            assertThat(failure).isNotNull();
            assertThat(failure.getCause().getCause()).isInstanceOf(MetadataStoreException.BadVersionException.class);
            assertThat(source.get(lockPath).join()).isPresent();

            setMigrationPhase(source, MigrationPhase.PREPARATION, targetUrl);
            assertThat(sessionEvents.poll(10, TimeUnit.SECONDS)).isEqualTo(SessionEvent.SessionLost);
            Awaitility.await().untilAsserted(() -> assertThat(target.get(lockPath).join()).isPresent());
            setMigrationPhase(source, MigrationPhase.COMPLETED, targetUrl);
            assertThat(sessionEvents.poll(10, TimeUnit.SECONDS)).isEqualTo(SessionEvent.SessionReestablished);
            if (close) {
                lum.close();
            } else {
                lum.releaseUnderreplicatedLedger(ledgerId);
            }
            assertThat(target.get(lockPath).join()).as("Source failure must leave the acquisition retryable").isEmpty();
        } finally {
            resumeDeletion.complete(null);
            source.delete(MigrationState.MIGRATION_FLAG_PATH, Optional.empty()).join();
        }
    }

    @DataProvider(name = "cleanupVersionConflict")
    public Object[][] cleanupVersionConflict() {
        List<Object[]> cases = new ArrayList<>(List.of(new Object[]{false, false}, new Object[]{false, true}));
        if (isOxiaEnabled()) {
            cases.addAll(List.of(new Object[]{true, false}, new Object[]{true, true}));
        }
        return cases.toArray(Object[][]::new);
    }

    @Test(timeOut = 60000, dataProvider = "cleanupVersionConflict")
    public void testLockCleanupAfterVersionConflict(boolean migrate, boolean close) throws Exception {
        AtomicReference<String> delayedPath = new AtomicReference<>();
        AtomicBoolean delayFirstDeletion = new AtomicBoolean(true);
        AtomicReference<Optional<Long>> submittedVersion = new AtomicReference<>();
        CountDownLatch deletionReady = new CountDownLatch(1);
        CompletableFuture<Void> resumeDeletion = new CompletableFuture<>();
        MetadataStoreExtended source = new ZKMetadataStore(zks.getConnectionString(),
                MetadataStoreConfig.builder().build(), true);
        // Delay submission after the real ownership read; the resumed delete uses the real backend.
        methodSetup(new DualMetadataStore(source, MetadataStoreConfig.builder().build()) {
            @Override
            public CompletableFuture<Void> delete(String path, Optional<Long> expectedVersion, Set<Option> options) {
                if (path.equals(delayedPath.get()) && delayFirstDeletion.compareAndSet(true, false)) {
                    submittedVersion.set(expectedVersion);
                    deletionReady.countDown();
                    return resumeDeletion.thenCompose(ignored -> super.delete(path, expectedVersion, options));
                }
                return super.delete(path, expectedVersion, options);
            }
        });
        long ledgerId = 123L;
        lum.acquireUnderreplicatedLedger(ledgerId);
        String lockPath = PulsarLedgerUnderreplicationManager.getUrLedgerLockPath(
                PulsarLedgerUnderreplicationManager.getUrLockPath(ledgersRoot), ledgerId);
        delayedPath.set(lockPath);
        GetResult acquired = source.get(lockPath).join().orElseThrow();
        String targetUrl = migrate ? "oxia://" + getOxiaServerConnectString() : null;
        CountDownLatch reestablished = new CountDownLatch(1);
        store.registerSessionListener(event -> {
            if (event == SessionEvent.SessionReestablished) {
                reestablished.countDown();
            }
        });
        try (MetadataStoreExtended target = migrate ? MetadataStoreExtended.create(targetUrl,
                MetadataStoreConfig.builder().build()) : null) {
            Future<UnavailableException> firstCleanup = executor.submit(() -> {
                try {
                    if (close) {
                        lum.close();
                    } else {
                        lum.releaseUnderreplicatedLedger(ledgerId);
                    }
                    return null;
                } catch (UnavailableException error) {
                    return error;
                }
            });
            try {
                assertThat(deletionReady.await(10, TimeUnit.SECONDS)).isTrue();
                assertThat(submittedVersion.get()).contains(acquired.getStat().getVersion());
                if (migrate) {
                    source.put(MigrationState.MIGRATION_FLAG_PATH,
                            ObjectMapperFactory.getMapper().writer().writeValueAsBytes(
                                    new MigrationState(MigrationPhase.PREPARATION, targetUrl)), Optional.empty())
                            .join();
                    Awaitility.await().untilAsserted(() -> {
                        assertThat(target.get(lockPath).join()).isPresent();
                        assertThat(source.getChildren(MigrationState.PARTICIPANTS_PATH).join()).isEmpty();
                    });
                    assertThat(target.get(lockPath).join().orElseThrow().getStat().getVersion())
                            .isNotEqualTo(acquired.getStat().getVersion());
                    source.put(MigrationState.MIGRATION_FLAG_PATH,
                            ObjectMapperFactory.getMapper().writer().writeValueAsBytes(
                                    new MigrationState(MigrationPhase.COPYING, targetUrl)), Optional.empty()).join();
                    source.put(MigrationState.MIGRATION_FLAG_PATH,
                            ObjectMapperFactory.getMapper().writer().writeValueAsBytes(
                                    new MigrationState(MigrationPhase.COMPLETED, targetUrl)), Optional.empty()).join();
                    assertThat(reestablished.await(10, TimeUnit.SECONDS)).isTrue();
                } else {
                    source.put(lockPath, acquired.getValue(), Optional.of(acquired.getStat().getVersion()),
                            EnumSet.of(CreateOption.Ephemeral)).join();
                }
            } finally {
                resumeDeletion.complete(null);
            }
            UnavailableException versionConflict = firstCleanup.get(10, TimeUnit.SECONDS);
            assertThat(versionConflict).isNotNull();
            assertThat(versionConflict.getCause().getCause())
                    .isInstanceOf(MetadataStoreException.BadVersionException.class);
            assertThat(store.get(lockPath).join()).isPresent();
            if (close) {
                lum.close();
            } else {
                lum.releaseUnderreplicatedLedger(ledgerId);
            }
            assertThat(store.get(lockPath).join())
                    .as("A version conflict must not discard cleanup ownership").isEmpty();
        } finally {
            resumeDeletion.complete(null);
            if (migrate && source.get(MigrationState.MIGRATION_FLAG_PATH).join().isPresent()) {
                source.delete(MigrationState.MIGRATION_FLAG_PATH, Optional.empty()).join();
            }
        }
    }

    @Test(timeOut = 60000, dataProvider = "migrationCleanup")
    public void testLockCleanupAfterMetadataMigration(boolean oxia, boolean close, boolean retry, boolean expireSource)
            throws Exception {
        methodSetup(zks::getConnectionString);
        long ledgerId = 123L;
        lum.acquireUnderreplicatedLedger(ledgerId);
        String lockPath = PulsarLedgerUnderreplicationManager.getUrLedgerLockPath(
                PulsarLedgerUnderreplicationManager.getUrLockPath(ledgersRoot), ledgerId);
        MetadataStoreExtended source = ((DualMetadataStore) store).getSourceStore();
        String targetUrl = oxia ? "oxia://" + getOxiaServerConnectString() : "memory:" + UUID.randomUUID();
        BlockingQueue<SessionEvent> sessionEvents = new LinkedBlockingQueue<>();
        store.registerSessionListener(sessionEvents::add);
        try (MetadataStoreExtended target = MetadataStoreExtended.create(targetUrl,
                MetadataStoreConfig.builder().build())) {
            source.put(MigrationState.MIGRATION_FLAG_PATH, ObjectMapperFactory.getMapper().writer().writeValueAsBytes(
                    new MigrationState(MigrationPhase.PREPARATION, targetUrl)), Optional.empty()).join();
            // Preparation recreates the ephemeral lock before acknowledging its participant.
            Awaitility.await().untilAsserted(() -> {
                assertThat(target.get(lockPath).join()).isPresent();
                assertThat(source.getChildren(MigrationState.PARTICIPANTS_PATH).join()).isEmpty();
            });
            assertThat(sessionEvents.poll(10, TimeUnit.SECONDS)).isEqualTo(SessionEvent.SessionLost);
            if (expireSource) {
                CountDownLatch sourceReestablished = new CountDownLatch(1);
                source.registerSessionListener(event -> {
                    if (event == SessionEvent.SessionReestablished) {
                        sourceReestablished.countDown();
                    }
                });
                zks.expireSession(((ZKMetadataStore) source).getZkSessionId());
                assertThat(sourceReestablished.await(20, TimeUnit.SECONDS)).isTrue();
                sessionEvents.clear();
                assertThat(source.get(lockPath).join()).isEmpty();
                assertThat(target.get(lockPath).join()).isPresent();
            }
            // Preparation is read-only. A completed failure must allow a fresh cleanup after cutover.
            assertThatThrownBy(() -> {
                if (close) {
                    lum.close();
                } else {
                    lum.releaseUnderreplicatedLedger(ledgerId);
                }
            }).isInstanceOf(UnavailableException.class);
            if (retry) {
                GetResult firstCopy = target.get(lockPath).join().orElseThrow();
                source.put(MigrationState.MIGRATION_FLAG_PATH,
                        ObjectMapperFactory.getMapper().writer().writeValueAsBytes(
                                new MigrationState(MigrationPhase.FAILED, targetUrl)), Optional.empty()).join();
                assertThat(sessionEvents.poll(10, TimeUnit.SECONDS)).isEqualTo(SessionEvent.SessionReestablished);
                source.put(MigrationState.MIGRATION_FLAG_PATH,
                        ObjectMapperFactory.getMapper().writer().writeValueAsBytes(
                                new MigrationState(MigrationPhase.PREPARATION, targetUrl)), Optional.empty()).join();
                assertThat(sessionEvents.poll(10, TimeUnit.SECONDS)).isEqualTo(SessionEvent.SessionLost);
                Awaitility.await().untilAsserted(() -> {
                    GetResult repeatedCopy = target.get(lockPath).join().orElseThrow();
                    assertThat(repeatedCopy.getStat().getVersion()).isGreaterThan(firstCopy.getStat().getVersion());
                    assertThat(repeatedCopy.getStat().isFirstVersion()).isFalse();
                    assertThat(repeatedCopy.getValue()).isEqualTo(firstCopy.getValue());
                });
            }
            source.put(MigrationState.MIGRATION_FLAG_PATH, ObjectMapperFactory.getMapper().writer().writeValueAsBytes(
                    new MigrationState(MigrationPhase.COMPLETED, targetUrl)), Optional.empty()).join();
            assertThat(sessionEvents.poll(10, TimeUnit.SECONDS)).isEqualTo(SessionEvent.SessionReestablished);
            assertThat(lum.isLedgerBeingReplicated(ledgerId)).isTrue();
            if (close) {
                lum.close();
            } else {
                lum.releaseUnderreplicatedLedger(ledgerId);
            }
            assertThat(target.get(lockPath).join()).as("The migrated lock must remain releasable").isEmpty();
        } finally {
            source.delete(MigrationState.MIGRATION_FLAG_PATH, Optional.empty()).join();
        }
    }

    @Test(timeOut = 60000)
    public void testLockAcquisitionAcrossSessionExpiration() throws Exception {
        AtomicReference<String> delayedPath = new AtomicReference<>();
        CountDownLatch acquisitionStarted = new CountDownLatch(1);
        CompletableFuture<Void> resumeCreation = new CompletableFuture<>();
        // Delay submission only: all creation, session expiration and cleanup use a real ZooKeeper store.
        ZKMetadataStore zkStore = new ZKMetadataStore(zks.getConnectionString(),
                MetadataStoreConfig.builder().build(), true);
        methodSetup(new DualMetadataStore(zkStore, MetadataStoreConfig.builder().build()) {
            @Override
            public CompletableFuture<Stat> put(String path, byte[] data, Optional<Long> expectedVersion,
                                               Set<Option> options) {
                if (path.equals(delayedPath.get())) {
                    acquisitionStarted.countDown();
                    return resumeCreation.thenCompose(ignored -> super.put(path, data, expectedVersion, options));
                }
                return super.put(path, data, expectedVersion, options);
            }
        });
        long ledgerId = 123L;
        String lockPath = PulsarLedgerUnderreplicationManager.getUrLedgerLockPath(
                PulsarLedgerUnderreplicationManager.getUrLockPath(ledgersRoot), ledgerId);
        delayedPath.set(lockPath);
        CountDownLatch reestablished = new CountDownLatch(1);
        store.registerSessionListener(event -> {
            if (event == SessionEvent.SessionReestablished) {
                reestablished.countDown();
            }
        });
        Future<?> acquisition = executor.submit(() -> {
            lum.acquireUnderreplicatedLedger(ledgerId);
            return null;
        });
        try {
            assertThat(acquisitionStarted.await(10, TimeUnit.SECONDS)).isTrue();
            zks.expireSession(zkStore.getZkSessionId());
            assertThat(reestablished.await(20, TimeUnit.SECONDS)).isTrue();
        } finally {
            resumeCreation.complete(null);
        }
        acquisition.get(10, TimeUnit.SECONDS);
        assertThat(store.get(lockPath).join()).isPresent();
        lum.releaseUnderreplicatedLedger(ledgerId);
        assertThat(store.get(lockPath).join()).as("A successful create in the new session must be tracked").isEmpty();
    }

    @Test(timeOut = 60000, dataProvider = "cleanupMethods")
    public void testLockCleanupAfterMigrationChangesDuringRead(boolean close) throws Exception {
        AtomicReference<String> delayedPath = new AtomicReference<>();
        AtomicBoolean delayFirstRead = new AtomicBoolean(true);
        CountDownLatch readStarted = new CountDownLatch(1);
        CountDownLatch sourceReadCompleted = new CountDownLatch(1);
        AtomicReference<Optional<GetResult>> sourceResult = new AtomicReference<>();
        CompletableFuture<Void> submitRead = new CompletableFuture<>();
        CompletableFuture<Void> deliverRead = new CompletableFuture<>();
        ZKMetadataStore source = new ZKMetadataStore(zks.getConnectionString(),
                MetadataStoreConfig.builder().build(), true);
        // Keep the real source read pending while migration changes the active store.
        methodSetup(new DualMetadataStore(source, MetadataStoreConfig.builder().build()) {
            @Override
            public CompletableFuture<Optional<GetResult>> get(String path, Set<Option> options) {
                if (path.equals(delayedPath.get()) && delayFirstRead.compareAndSet(true, false)) {
                    readStarted.countDown();
                    return submitRead.thenCompose(ignored -> super.get(path, options)).thenCompose(value -> {
                        sourceResult.set(value);
                        sourceReadCompleted.countDown();
                        return deliverRead.thenApply(ignored -> value);
                    });
                }
                return super.get(path, options);
            }
        });
        long ledgerId = 123L;
        lum.acquireUnderreplicatedLedger(ledgerId);
        String lockPath = PulsarLedgerUnderreplicationManager.getUrLedgerLockPath(
                PulsarLedgerUnderreplicationManager.getUrLockPath(ledgersRoot), ledgerId);
        delayedPath.set(lockPath);
        String targetUrl = "memory:" + UUID.randomUUID();
        try (MetadataStoreExtended target = MetadataStoreExtended.create(targetUrl,
                MetadataStoreConfig.builder().build())) {
            Future<UnavailableException> firstCleanup = executor.submit(() -> {
                try {
                    if (close) {
                        lum.close();
                    } else {
                        lum.releaseUnderreplicatedLedger(ledgerId);
                    }
                    return null;
                } catch (UnavailableException error) {
                    return error;
                }
            });
            assertThat(readStarted.await(10, TimeUnit.SECONDS)).isTrue();
            source.put(MigrationState.MIGRATION_FLAG_PATH,
                    ObjectMapperFactory.getMapper().writer().writeValueAsBytes(
                            new MigrationState(MigrationPhase.PREPARATION, targetUrl)), Optional.empty()).join();
            Awaitility.await().untilAsserted(() -> {
                assertThat(target.get(lockPath).join()).isPresent();
                assertThat(source.getChildren(MigrationState.PARTICIPANTS_PATH).join()).isEmpty();
            });
            CountDownLatch sourceReestablished = new CountDownLatch(1);
            source.registerSessionListener(event -> {
                if (event == SessionEvent.SessionReestablished) {
                    sourceReestablished.countDown();
                }
            });
            zks.expireSession(source.getZkSessionId());
            assertThat(sourceReestablished.await(20, TimeUnit.SECONDS)).isTrue();
            submitRead.complete(null);
            assertThat(sourceReadCompleted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(sourceResult.get()).isEmpty();
            CountDownLatch migrationCompleted = new CountDownLatch(1);
            store.registerSessionListener(event -> {
                if (event == SessionEvent.SessionReestablished) {
                    migrationCompleted.countDown();
                }
            });
            source.put(MigrationState.MIGRATION_FLAG_PATH,
                    ObjectMapperFactory.getMapper().writer().writeValueAsBytes(
                            new MigrationState(MigrationPhase.COMPLETED, targetUrl)), Optional.empty()).join();
            assertThat(migrationCompleted.await(10, TimeUnit.SECONDS)).isTrue();
            deliverRead.complete(null);
            assertThat(firstCleanup.get(10, TimeUnit.SECONDS)).isNotNull();
            assertThat(target.get(lockPath).join()).isPresent();
            if (close) {
                lum.close();
            } else {
                lum.releaseUnderreplicatedLedger(ledgerId);
            }
            assertThat(target.get(lockPath).join()).isEmpty();
        } finally {
            submitRead.complete(null);
            deliverRead.complete(null);
            if (source.get(MigrationState.MIGRATION_FLAG_PATH).join().isPresent()) {
                source.delete(MigrationState.MIGRATION_FLAG_PATH, Optional.empty()).join();
            }
        }
    }

    @Test(timeOut = 60000)
    public void testMarkLedgerReplicatedAfterSessionExpiration() throws Exception {
        methodSetup(zks::getConnectionString);
        long ledgerId = 123L;
        lum.markLedgerUnderreplicated(ledgerId, "bookie:3181");
        assertThat(lum.pollLedgerToRereplicate()).isEqualTo(ledgerId);
        CountDownLatch reestablished = new CountDownLatch(1);
        store.registerSessionListener(event -> {
            if (event == SessionEvent.SessionReestablished) {
                reestablished.countDown();
            }
        });
        ZKMetadataStore zkStore = (ZKMetadataStore) ((DualMetadataStore) store).getSourceStore();
        zks.expireSession(zkStore.getZkSessionId());
        assertThat(reestablished.await(20, TimeUnit.SECONDS)).isTrue();
        try (var other = lmf.newLedgerUnderreplicationManager()) {
            assertThat(other.pollLedgerToRereplicate()).isEqualTo(ledgerId);
            lum.markLedgerReplicated(ledgerId);
            assertThat(lum.getLedgerUnreplicationInfo(ledgerId)).isNotNull();
            assertThat(other.isLedgerBeingReplicated(ledgerId)).isTrue();
            other.markLedgerReplicated(ledgerId);
            assertThat(lum.getLedgerUnreplicationInfo(ledgerId)).isNull();
        }
    }

    @Test(timeOut = 30000, dataProvider = "impl")
    public void testLockCleanupPreservesReplacementInSameSession(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);
        long ledgerId = 123L;
        lum.acquireUnderreplicatedLedger(ledgerId);
        String lockPath = PulsarLedgerUnderreplicationManager.getUrLedgerLockPath(
                PulsarLedgerUnderreplicationManager.getUrLockPath(ledgersRoot), ledgerId);
        GetResult original = store.get(lockPath).join().orElseThrow();
        store.delete(lockPath, Optional.of(original.getStat().getVersion())).join();
        try (var other = lmf.newLedgerUnderreplicationManager()) {
            other.acquireUnderreplicatedLedger(ledgerId);
            lum.releaseUnderreplicatedLedger(ledgerId);
            lum.close();
            assertThat(other.isLedgerBeingReplicated(ledgerId))
                    .as("Store session ownership must not conflate separate lock acquisitions").isTrue();
        }
    }

    @Test(timeOut = 60000, dataProvider = "impl")
    public void testLockCleanupVersionFenceAcrossReplacement(String provider, Supplier<String> urlSupplier)
            throws Exception {
        AtomicReference<String> delayedPath = new AtomicReference<>();
        AtomicBoolean delayFirstDeletion = new AtomicBoolean(true);
        AtomicReference<Optional<Long>> submittedVersion = new AtomicReference<>();
        CountDownLatch deletionReady = new CountDownLatch(1);
        CompletableFuture<Void> resumeDeletion = new CompletableFuture<>();
        MetadataStoreConfig config = MetadataStoreConfig.builder().fsyncEnable(false).build();
        MetadataStoreExtended source = "ZooKeeper".equals(provider)
                ? new ZKMetadataStore(urlSupplier.get(), config, true)
                : MetadataStoreFactoryImpl.createExtended(urlSupplier.get(), config);
        // Gate only submission: the ownership read, replacement and resumed delete use the real backend.
        methodSetup(new DualMetadataStore(source, MetadataStoreConfig.builder().build()) {
            @Override
            public CompletableFuture<Void> delete(String path, Optional<Long> expectedVersion, Set<Option> options) {
                if (path.equals(delayedPath.get()) && delayFirstDeletion.compareAndSet(true, false)) {
                    submittedVersion.set(expectedVersion);
                    deletionReady.countDown();
                    return resumeDeletion.thenCompose(ignored -> super.delete(path, expectedVersion, options));
                }
                return super.delete(path, expectedVersion, options);
            }
        });
        long ledgerId = 123L;
        lum.acquireUnderreplicatedLedger(ledgerId);
        String lockPath = PulsarLedgerUnderreplicationManager.getUrLedgerLockPath(
                PulsarLedgerUnderreplicationManager.getUrLockPath(ledgersRoot), ledgerId);
        delayedPath.set(lockPath);
        GetResult acquired = source.get(lockPath).join().orElseThrow();
        Future<UnavailableException> cleanup = executor.submit(() -> {
            try {
                lum.releaseUnderreplicatedLedger(ledgerId);
                return null;
            } catch (UnavailableException error) {
                return error;
            }
        });
        try (var other = lmf.newLedgerUnderreplicationManager()) {
            try {
                assertThat(deletionReady.await(10, TimeUnit.SECONDS)).isTrue();
                assertThat(submittedVersion.get()).contains(acquired.getStat().getVersion());
                if ("ZooKeeper".equals(provider)) {
                    CountDownLatch reestablished = new CountDownLatch(1);
                    source.registerSessionListener(event -> {
                        if (event == SessionEvent.SessionReestablished) {
                            reestablished.countDown();
                        }
                    });
                    zks.expireSession(((ZKMetadataStore) source).getZkSessionId());
                    assertThat(reestablished.await(20, TimeUnit.SECONDS)).isTrue();
                } else {
                    source.delete(lockPath, Optional.of(acquired.getStat().getVersion())).join();
                }
                other.acquireUnderreplicatedLedger(ledgerId);
                GetResult replacement = source.get(lockPath).join().orElseThrow();
                assertThat(replacement.getValue()).isNotEqualTo(acquired.getValue());
                if ("Oxia".equals(provider)) {
                    assertThat(replacement.getStat().getVersion()).isNotEqualTo(acquired.getStat().getVersion());
                } else {
                    assertThat(replacement.getStat().getVersion()).isEqualTo(acquired.getStat().getVersion());
                }
            } finally {
                resumeDeletion.complete(null);
            }
            UnavailableException failure = cleanup.get(10, TimeUnit.SECONDS);
            if ("Oxia".equals(provider)) {
                assertThat(failure).isNotNull();
                assertThat(failure.getCause().getCause())
                        .isInstanceOf(MetadataStoreException.BadVersionException.class);
                lum.releaseUnderreplicatedLedger(ledgerId);
                assertThat(other.isLedgerBeingReplicated(ledgerId)).isTrue();
            } else {
                // These backends reuse the version after recreation, so the conditional delete cannot fence
                // a replacement made after the ownership read. This test pins the documented boundary.
                assertThat(failure).isNull();
                assertThat(other.isLedgerBeingReplicated(ledgerId)).isFalse();
            }
        } finally {
            resumeDeletion.complete(null);
        }
    }

    @Test(timeOut = 30000)
    public void testLockDataCompatibilityWithBookKeeper() throws Exception {
        methodSetup(zks::getConnectionString);
        long ledgerId = 123L;
        lum.acquireUnderreplicatedLedger(ledgerId);
        ClientConfiguration conf = new ClientConfiguration();
        conf.setZkLedgersRootPath(ledgersRoot);
        try (ZooKeeperClient client = ZooKeeperClient.newBuilder().connectString(zks.getConnectionString()).build();
             var bookKeeperManager = new ZkLedgerUnderreplicationManager(conf, client)) {
            assertThat(bookKeeperManager.getReplicationWorkerIdRereplicatingLedger(ledgerId))
                    .isEqualTo(DNS.getDefaultHost("default"));
            assertThat(bookKeeperManager.isLedgerBeingReplicated(ledgerId)).isTrue();
        }
        lum.releaseUnderreplicatedLedger(ledgerId);
    }

    @Test(timeOut = 60000, dataProvider = "cleanupMethods")
    public void testLockCleanupBeforeSessionLostNotification(boolean close) throws Exception {
        methodSetup(zks::getConnectionString);
        long ledgerId = 123L;
        lum.acquireUnderreplicatedLedger(ledgerId);
        String lockPath = PulsarLedgerUnderreplicationManager.getUrLedgerLockPath(
                PulsarLedgerUnderreplicationManager.getUrLockPath(ledgersRoot), ledgerId);
        String barrierPath = "/session-callback-barrier-" + UUID.randomUUID();
        CountDownLatch callbackBlocked = new CountDownLatch(1);
        CountDownLatch resumeCallbacks = new CountDownLatch(1);
        store.registerListener(notification -> {
            if (notification.getPath().equals(barrierPath)
                    && notification.getType() == NotificationType.Created) {
                callbackBlocked.countDown();
                try {
                    assertThat(resumeCallbacks.await(40, TimeUnit.SECONDS)).isTrue();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        try (MetadataStoreExtended otherStore = MetadataStoreExtended.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().build());
             var other = new PulsarLedgerUnderreplicationManager(new ClientConfiguration(), otherStore, ledgersRoot)) {
            ZKMetadataStore zkStore = (ZKMetadataStore) ((DualMetadataStore) store).getSourceStore();
            long expiredSession = zkStore.getZkSessionId();
            CompletableFuture<?> barrierWrite = store.put(barrierPath, new byte[0], Optional.of(-1L));
            Future<?> cleanup;
            try {
                assertThat(callbackBlocked.await(10, TimeUnit.SECONDS)).isTrue();
                zks.expireSession(expiredSession);
                Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() ->
                        zkStore.getZkSessionId() != expiredSession && zkStore.getZkSessionId() != 0);
                Awaitility.await().untilAsserted(() -> assertThat(otherStore.get(lockPath).join()).isEmpty());
                other.acquireUnderreplicatedLedger(ledgerId);

                AtomicReference<Thread> cleanupThread = new AtomicReference<>();
                cleanup = executor.submit(() -> {
                    cleanupThread.set(Thread.currentThread());
                    if (close) {
                        lum.close();
                    } else {
                        lum.releaseUnderreplicatedLedger(ledgerId);
                    }
                    return null;
                });
                Awaitility.await().until(() -> cleanupThread.get() != null
                        && cleanupThread.get().getState() == Thread.State.TIMED_WAITING);
            } finally {
                resumeCallbacks.countDown();
            }
            barrierWrite.get(10, TimeUnit.SECONDS);
            cleanup.get(10, TimeUnit.SECONDS);
            assertThat(other.isLedgerBeingReplicated(ledgerId))
                    .as("Delayed session notifications must not let cleanup delete another session's lock").isTrue();
        } finally {
            resumeCallbacks.countDown();
        }
    }

    @Test(timeOut = 30000, dataProvider = "impl")
    public void testLockCleanupPreservesModifiedLock(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);
        long ledgerId = 123L;
        lum.acquireUnderreplicatedLedger(ledgerId);
        String lockPath = PulsarLedgerUnderreplicationManager.getUrLedgerLockPath(
                PulsarLedgerUnderreplicationManager.getUrLockPath(ledgersRoot), ledgerId);
        GetResult acquired = store.get(lockPath).get(10, TimeUnit.SECONDS).orElseThrow();
        // A compatible writer replaces the payload, removing this acquisition's identity.
        store.put(lockPath, PulsarLedgerUnderreplicationManager.getLockData(),
                Optional.of(acquired.getStat().getVersion()))
                .get(10, TimeUnit.SECONDS);
        GetResult modified = store.get(lockPath).get(10, TimeUnit.SECONDS).orElseThrow();
        try {
            lum.releaseUnderreplicatedLedger(ledgerId);
            lum.close();
            assertThat(store.get(lockPath).get(10, TimeUnit.SECONDS)).isPresent().get()
                    .extracting(result -> result.getStat().getVersion()).isEqualTo(modified.getStat().getVersion());
        } finally {
            if (store.get(lockPath).get(10, TimeUnit.SECONDS).isPresent()) {
                store.delete(lockPath, Optional.of(modified.getStat().getVersion())).get(10, TimeUnit.SECONDS);
            }
        }
    }

    @Test(timeOut = 30000, dataProvider = "impl")
    public void testLockCleanupAfterVersionUpdate(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);
        long ledgerId = 123L;
        lum.acquireUnderreplicatedLedger(ledgerId);
        String lockPath = PulsarLedgerUnderreplicationManager.getUrLedgerLockPath(
                PulsarLedgerUnderreplicationManager.getUrLockPath(ledgersRoot), ledgerId);
        GetResult acquired = store.get(lockPath).join().orElseThrow();
        store.put(lockPath, acquired.getValue(), Optional.of(acquired.getStat().getVersion()),
                EnumSet.of(CreateOption.Ephemeral)).join();
        Stat updated = store.get(lockPath).join().orElseThrow().getStat();
        assertThat(updated.getVersion()).isGreaterThan(acquired.getStat().getVersion());
        assertThat(updated.isEphemeral()).isTrue();
        assertThat(updated.isCreatedBySelf()).isTrue();
        lum.releaseUnderreplicatedLedger(ledgerId);
        assertThat(store.get(lockPath).join())
                .as("A version update must preserve the acquisition's identity").isEmpty();
    }

    @Test(timeOut = 30000, dataProvider = "impl")
    public void testExplicitLockRelease(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);
        long ledgerId = 123L;
        try (var other = lmf.newLedgerUnderreplicationManager()) {
            assertThat(lum.getLedgerUnreplicationInfo(ledgerId)).isNull();
            lum.acquireUnderreplicatedLedger(ledgerId);
            assertThat(other.isLedgerBeingReplicated(ledgerId)).isTrue();

            lum.releaseUnderreplicatedLedger(ledgerId);
            assertThat(other.isLedgerBeingReplicated(ledgerId)).isFalse();
            other.acquireUnderreplicatedLedger(ledgerId);

            // Releasing again must not remove the new owner's lock.
            lum.releaseUnderreplicatedLedger(ledgerId);
            assertThat(other.isLedgerBeingReplicated(ledgerId)).isTrue();
            other.releaseUnderreplicatedLedger(ledgerId);
            assertThat(lum.isLedgerBeingReplicated(ledgerId)).isFalse();
        }
    }

    @Test(timeOut = 30000, dataProvider = "impl")
    public void testExplicitLockCloseWithSharedStore(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);
        long polledLedgerId = 123L;
        long explicitLedgerId = 456L;
        try (var other = lmf.newLedgerUnderreplicationManager()) {
            lum.markLedgerUnderreplicated(polledLedgerId, "bookie:3181");
            assertThat(lum.pollLedgerToRereplicate()).isEqualTo(polledLedgerId);
            lum.acquireUnderreplicatedLedger(explicitLedgerId);

            lum.close();

            // Both managers share the same store, which remains open after lum.close().
            assertThat(other.isLedgerBeingReplicated(polledLedgerId)).isFalse();
            assertThat(other.isLedgerBeingReplicated(explicitLedgerId)).isFalse();
            other.acquireUnderreplicatedLedger(explicitLedgerId);
            assertThat(other.pollLedgerToRereplicate()).isEqualTo(polledLedgerId);
            other.releaseUnderreplicatedLedger(explicitLedgerId);
            other.markLedgerReplicated(polledLedgerId);
        }
    }

    @Test(timeOut = 30000, dataProvider = "impl")
    public void testExplicitLockMarkPreservesUnderreplicatedRecord(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);
        long ledgerId = 123L;
        lum.markLedgerUnderreplicated(ledgerId, "bookie:3181");
        String ledgerPath = PulsarLedgerUnderreplicationManager.getUrLedgerPath(urLedgerPath, ledgerId);
        GetResult original = store.get(ledgerPath).get(10, TimeUnit.SECONDS).orElseThrow();

        lum.acquireUnderreplicatedLedger(ledgerId);
        lum.markLedgerReplicated(ledgerId);

        GetResult retained = store.get(ledgerPath).get(10, TimeUnit.SECONDS).orElseThrow();
        assertThat(retained.getValue()).isEqualTo(original.getValue());
        assertThat(retained.getStat().getVersion()).isEqualTo(original.getStat().getVersion());
        assertThat(lum.isLedgerBeingReplicated(ledgerId)).isFalse();
        try (var other = lmf.newLedgerUnderreplicationManager()) {
            assertThat(other.pollLedgerToRereplicate()).isEqualTo(ledgerId);
            other.markLedgerReplicated(ledgerId);
            assertThat(other.getLedgerUnreplicationInfo(ledgerId)).isNull();
            assertThat(other.isLedgerBeingReplicated(ledgerId)).isFalse();
        }
    }

    @Test(timeOut = 30000, dataProvider = "impl")
    public void testExplicitLockMarkWithoutUnderreplicatedRecord(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);
        long ledgerId = 123L;
        lum.acquireUnderreplicatedLedger(ledgerId);
        lum.markLedgerReplicated(ledgerId);

        assertThat(lum.getLedgerUnreplicationInfo(ledgerId)).isNull();
        assertThat(lum.isLedgerBeingReplicated(ledgerId)).isFalse();
        try (var other = lmf.newLedgerUnderreplicationManager()) {
            other.acquireUnderreplicatedLedger(ledgerId);
            other.releaseUnderreplicatedLedger(ledgerId);
        }
    }

    @Test(timeOut = 30000, dataProvider = "impl")
    public void testFailedExplicitLockAcquireCannotReleaseOwner(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);
        long ledgerId = 123L;
        lum.markLedgerUnderreplicated(ledgerId, "bookie:3181");
        assertThat(lum.pollLedgerToRereplicate()).isEqualTo(ledgerId);
        try (var other = lmf.newLedgerUnderreplicationManager()) {
            assertThatThrownBy(() -> other.acquireUnderreplicatedLedger(ledgerId))
                    .isInstanceOf(UnavailableException.class);
            other.releaseUnderreplicatedLedger(ledgerId);
            assertThat(lum.isLedgerBeingReplicated(ledgerId)).isTrue();
            other.markLedgerReplicated(ledgerId);
            assertThat(lum.getLedgerUnreplicationInfo(ledgerId)).isNotNull();
            other.close();
            assertThat(lum.isLedgerBeingReplicated(ledgerId)).isTrue();
        }
        lum.markLedgerReplicated(ledgerId);
        assertThat(lum.getLedgerUnreplicationInfo(ledgerId)).isNull();
        assertThat(lum.isLedgerBeingReplicated(ledgerId)).isFalse();
    }

    @Test(timeOut = 30000, dataProvider = "impl")
    public void testFailedExplicitLockAcquirePreservesPolledVersion(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);
        long ledgerId = 123L;
        lum.markLedgerUnderreplicated(ledgerId, "bookie:3181");
        assertThat(lum.pollLedgerToRereplicate()).isEqualTo(ledgerId);
        assertThatThrownBy(() -> lum.acquireUnderreplicatedLedger(ledgerId))
                .isInstanceOf(UnavailableException.class);

        lum.markLedgerReplicated(ledgerId);

        assertThat(lum.getLedgerUnreplicationInfo(ledgerId)).isNull();
        assertThat(lum.isLedgerBeingReplicated(ledgerId)).isFalse();
    }

    /**
     * Test basic interactions with the ledger underreplication
     * manager.
     * Mark some ledgers as underreplicated.
     * Ensure that getLedgerToReplicate will block until it a ledger
     * becomes available.
     */
    @Test(timeOut = 60000, dataProvider = "impl")
    public void testBasicInteraction(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);

        Set<Long> ledgers = new HashSet<>();
        ledgers.add(0xdeadbeefL);
        ledgers.add(0xbeefcafeL);
        ledgers.add(0xffffbeefL);
        ledgers.add(0xfacebeefL);
        String missingReplica = "localhost:3181";

        int count = ledgers.size();
        for (long l : ledgers) {
            lum.markLedgerUnderreplicated(l, missingReplica);
        }

        List<Future<Long>> futures = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            futures.add(getLedgerToReplicate(lum));
        }

        for (Future<Long> f : futures) {
            Long l = f.get(5, TimeUnit.SECONDS);
            assertTrue(ledgers.remove(l));
        }

        Future<Long> f = getLedgerToReplicate(lum);
        try {
            f.get(1, TimeUnit.SECONDS);
            fail("Shouldn't be able to find a ledger to replicate");
        } catch (TimeoutException te) {
            // correct behaviour
        }
        Long newl = 0xfefefefefefeL;
        lum.markLedgerUnderreplicated(newl, missingReplica);
        assertEquals(f.get(5, TimeUnit.SECONDS), newl, "Should have got the one just added");
    }

    @Test(timeOut = 60000, dataProvider = "impl")
    public void testGetList(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);

        Set<Long> ledgers = new HashSet<>();
        ledgers.add(0xdeadbeefL);
        ledgers.add(0xbeefcafeL);
        ledgers.add(0xffffbeefL);
        ledgers.add(0xfacebeefL);
        String missingReplica = "localhost:3181";

        for (long l : ledgers) {
            lum.markLedgerUnderreplicated(l, missingReplica);
        }

        Set<Long> foundLedgers = new HashSet<>();
        for (Iterator<UnderreplicatedLedger> it = lum.listLedgersToRereplicate(null); it.hasNext(); ) {
            UnderreplicatedLedger ul = it.next();
            foundLedgers.add(ul.getLedgerId());
        }

        assertEquals(foundLedgers, ledgers);
    }

    /**
     * Test locking for ledger unreplication manager.
     * If there's only one ledger marked for rereplication,
     * and one client has it, it should be locked; another
     * client shouldn't be able to get it. If the first client dies
     * however, the second client should be able to get it.
     */
    @Test(timeOut = 60000, dataProvider = "impl")
    public void testLocking(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);

        String missingReplica = "localhost:3181";

        LedgerUnderreplicationManager m1 = lmf.newLedgerUnderreplicationManager();

        @Cleanup
        LedgerUnderreplicationManager m2 = lmf.newLedgerUnderreplicationManager();

        Long ledger = 0xfeadeefdacL;
        m1.markLedgerUnderreplicated(ledger, missingReplica);
        Future<Long> f = getLedgerToReplicate(m1);
        Long l = f.get(5, TimeUnit.SECONDS);
        assertEquals(l, ledger, "Should be the ledger I just marked");

        f = getLedgerToReplicate(m2);
        try {
            f.get(1, TimeUnit.SECONDS);
            fail("Shouldn't be able to find a ledger to replicate");
        } catch (TimeoutException te) {
            // correct behaviour
        }

        // Release the lock
        m1.close();

        l = f.get(5, TimeUnit.SECONDS);
        assertEquals(l, ledger, "Should be the ledger I marked");
    }


    /**
     * Test that when a ledger has been marked as replicated, it
     * will not be offered to another client.
     * This test checked that by marking two ledgers, and acquiring
     * them on a single client. It marks one as replicated and then
     * the client is killed. We then check that another client can
     * acquire a ledger, and that it's not the one that was previously
     * marked as replicated.
     */
    @Test(timeOut = 240000, dataProvider = "impl")
    public void testMarkingAsReplicated(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);

        String missingReplica = "localhost:3181";

        LedgerUnderreplicationManager m1 = lmf.newLedgerUnderreplicationManager();

        @Cleanup
        LedgerUnderreplicationManager m2 = lmf.newLedgerUnderreplicationManager();

        Long ledgerA = 0xfeadeefdacL;
        Long ledgerB = 0xdefadebL;
        m1.markLedgerUnderreplicated(ledgerA, missingReplica);
        m1.markLedgerUnderreplicated(ledgerB, missingReplica);

        AtomicReference<Long> lA = new AtomicReference<>();
        AtomicReference<Long> lB = new AtomicReference<>();

        Awaitility.await().untilAsserted(() -> {
            Future<Long> fA = getLedgerToReplicate(m1);
            Future<Long> fB = getLedgerToReplicate(m1);

            Long a = fA.get(5, TimeUnit.SECONDS);
            Long b = fB.get(5, TimeUnit.SECONDS);

            assertTrue((a.equals(ledgerA) && b.equals(ledgerB)) || (a.equals(ledgerB) && b.equals(ledgerA)),
                    "Should be the ledgers I just marked");
            lA.set(a);
            lB.set(b);
        });
        Future<Long> f = getLedgerToReplicate(m2);
        try {
            f.get(1, TimeUnit.SECONDS);
            fail("Shouldn't be able to find a ledger to replicate");
        } catch (TimeoutException te) {
            // correct behaviour
        }
        m1.markLedgerReplicated(lA.get());

        // Release the locks
        m1.close();

        Long l = f.get(5, TimeUnit.SECONDS);
        assertEquals(l, lB.get(), "Should be the ledger I marked");
    }

    @Test(dataProvider = "distributedImpl", timeOut = 10000)
    public void testMarkReplicatedDeletesEmptyParentNodes(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);

        String missingReplica = "localhost:3181";

        @Cleanup
        LedgerUnderreplicationManager m1 = lmf.newLedgerUnderreplicationManager();

        Long ledgerA = 0xfeadeefdacL;
        m1.markLedgerUnderreplicated(ledgerA, missingReplica);

        Field storeField = m1.getClass().getDeclaredField("store");
        storeField.setAccessible(true);
        MetadataStoreExtended metadataStore = (MetadataStoreExtended) storeField.get(m1);

        String fiveLevelPath = PulsarLedgerUnderreplicationManager.getUrLedgerPath(urLedgerPath, ledgerA);
        Optional<GetResult> getResult = metadataStore.get(fiveLevelPath).get(1, TimeUnit.SECONDS);
        assertTrue(getResult.isPresent());

        String fourLevelPath = fiveLevelPath.substring(0, fiveLevelPath.lastIndexOf("/"));
        getResult = metadataStore.get(fourLevelPath).get(1, TimeUnit.SECONDS);
        assertTrue(getResult.isPresent());

        String threeLevelPath = fourLevelPath.substring(0, fourLevelPath.lastIndexOf("/"));
        getResult = metadataStore.get(threeLevelPath).get(1, TimeUnit.SECONDS);
        assertTrue(getResult.isPresent());

        String twoLevelPath = fourLevelPath.substring(0, threeLevelPath.lastIndexOf("/"));
        getResult = metadataStore.get(twoLevelPath).get(1, TimeUnit.SECONDS);
        assertTrue(getResult.isPresent());

        String oneLevelPath = fourLevelPath.substring(0, twoLevelPath.lastIndexOf("/"));
        getResult = metadataStore.get(oneLevelPath).get(1, TimeUnit.SECONDS);
        assertTrue(getResult.isPresent());

        getResult = metadataStore.get(urLedgerPath).get(1, TimeUnit.SECONDS);
        assertTrue(getResult.isPresent());

        long ledgerToRereplicate = m1.getLedgerToRereplicate();
        assertEquals(ledgerToRereplicate, ledgerA);
        m1.markLedgerReplicated(ledgerA);

        getResult = metadataStore.get(fiveLevelPath).get(1, TimeUnit.SECONDS);
        assertFalse(getResult.isPresent());

        getResult = metadataStore.get(fourLevelPath).get(1, TimeUnit.SECONDS);
        assertFalse(getResult.isPresent());

        getResult = metadataStore.get(threeLevelPath).get(1, TimeUnit.SECONDS);
        assertFalse(getResult.isPresent());

        getResult = metadataStore.get(twoLevelPath).get(1, TimeUnit.SECONDS);
        assertFalse(getResult.isPresent());

        getResult = metadataStore.get(oneLevelPath).get(1, TimeUnit.SECONDS);
        assertFalse(getResult.isPresent());

        getResult = metadataStore.get(urLedgerPath).get(1, TimeUnit.SECONDS);
        assertTrue(getResult.isPresent());
    }

    /**
     * Test releasing of a ledger
     * A ledger is released when a client decides it does not want
     * to replicate it (or cannot at the moment).
     * When a client releases a previously acquired ledger, another
     * client should then be able to acquire it.
     */
    @Test(timeOut = 60000, dataProvider = "impl")
    public void testRelease(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);

        String missingReplica = "localhost:3181";

        @Cleanup
        LedgerUnderreplicationManager m1 = lmf.newLedgerUnderreplicationManager();

        @Cleanup
        LedgerUnderreplicationManager m2 = lmf.newLedgerUnderreplicationManager();

        Long ledgerA = 0xfeadeefdacL;
        Long ledgerB = 0xdefadebL;
        m1.markLedgerUnderreplicated(ledgerA, missingReplica);
        m1.markLedgerUnderreplicated(ledgerB, missingReplica);

        Future<Long> fA = getLedgerToReplicate(m1);
        Future<Long> fB = getLedgerToReplicate(m1);

        Long lA = fA.get(5, TimeUnit.SECONDS);
        Long lB = fB.get(5, TimeUnit.SECONDS);

        assertTrue((lA.equals(ledgerA) && lB.equals(ledgerB)) || (lA.equals(ledgerB) && lB.equals(ledgerA)),
                "Should be the ledgers I just marked");

        Future<Long> f = getLedgerToReplicate(m2);
        try {
            f.get(1, TimeUnit.SECONDS);
            fail("Shouldn't be able to find a ledger to replicate");
        } catch (TimeoutException te) {
            // correct behaviour
        }
        m1.markLedgerReplicated(lA);
        m1.releaseUnderreplicatedLedger(lB);

        Long l = f.get(5, TimeUnit.SECONDS);
        assertEquals(l, lB, "Should be the ledger I marked");
    }

    /**
     * Test that when a failure occurs on a ledger, while the ledger
     * is already being rereplicated, the ledger will still be in the
     * under replicated ledger list when first rereplicating client marks
     * it as replicated.
     */
    @Test(timeOut = 60000, dataProvider = "impl")
    public void testManyFailures(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);

        String missingReplica1 = "localhost:3181";
        String missingReplica2 = "localhost:3182";

        Long ledgerA = 0xfeadeefdacL;
        lum.markLedgerUnderreplicated(ledgerA, missingReplica1);

        Future<Long> fA = getLedgerToReplicate(lum);
        Long lA = fA.get(5, TimeUnit.SECONDS);

        lum.markLedgerUnderreplicated(ledgerA, missingReplica2);

        assertEquals(lA, ledgerA, "Should be the ledger I just marked");
        lum.markLedgerReplicated(lA);

        Future<Long> f = getLedgerToReplicate(lum);
        lA = f.get(5, TimeUnit.SECONDS);
        assertEquals(lA, ledgerA, "Should be the ledger I had marked previously");
    }

    /**
     * If replicationworker has acquired lock on it, then
     * getReplicationWorkerIdRereplicatingLedger should return
     * ReplicationWorkerId (BookieId) of the ReplicationWorker that is holding
     * lock. If lock for the underreplicated ledger is not yet acquired or if it
     * is released then it is supposed to return null.
     *
     * @throws Exception
     */
    @Test(timeOut = 60000, dataProvider = "impl")
    public void testGetReplicationWorkerIdRereplicatingLedger(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);
        String missingReplica1 = "localhost:3181";
        String missingReplica2 = "localhost:3182";

        Long ledgerA = 0xfeadeefdacL;
        lum.markLedgerUnderreplicated(ledgerA, missingReplica1);
        lum.markLedgerUnderreplicated(ledgerA, missingReplica2);

        // lock is not yet acquired so replicationWorkerIdRereplicatingLedger
        // should
        assertEquals(lum.getReplicationWorkerIdRereplicatingLedger(ledgerA), null, "ReplicationWorkerId of the lock");

        Future<Long> fA = getLedgerToReplicate(lum);
        Long lA = fA.get(5, TimeUnit.SECONDS);
        assertEquals(lA, ledgerA, "Should be the ledger that was just marked");

        /*
         * ZkLedgerUnderreplicationManager.getLockData uses
         * DNS.getDefaultHost("default") as the bookieId.
         *
         */
        assertEquals(lum.getReplicationWorkerIdRereplicatingLedger(ledgerA), DNS.getDefaultHost("default"),
                "ReplicationWorkerId of the lock");

        lum.markLedgerReplicated(lA);

        assertEquals(lum.getReplicationWorkerIdRereplicatingLedger(ledgerA), null, "ReplicationWorkerId of the lock");
    }

    /**
     * Test that when a ledger is marked as underreplicated with
     * the same missing replica twice, only marking as replicated
     * will be enough to remove it from the list.
     */
    @Test(timeOut = 60000, dataProvider = "impl")
    public void test2reportSame(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);
        String missingReplica1 = "localhost:3181";

        LedgerUnderreplicationManager m1 = lmf.newLedgerUnderreplicationManager();
        LedgerUnderreplicationManager m2 = lmf.newLedgerUnderreplicationManager();

        Long ledgerA = 0xfeadeefdacL;
        m1.markLedgerUnderreplicated(ledgerA, missingReplica1);
        m2.markLedgerUnderreplicated(ledgerA, missingReplica1);

        // verify duplicate missing replica
        UnderreplicatedLedgerFormat builderA = new UnderreplicatedLedgerFormat();
        byte[] data = store.get(getUrLedgerZnode(ledgerA)).join().get().getValue();
        builderA.parseFromTextFormat(data);
        List<String> replicaList = builderA.getReplicasList();
        assertEquals(replicaList.size(), 1, "Published duplicate missing replica : " + replicaList);
        assertTrue(replicaList.contains(missingReplica1), "Published duplicate missing replica : " + replicaList);

        Future<Long> fA = getLedgerToReplicate(m1);
        Long lA = fA.get(5, TimeUnit.SECONDS);

        assertEquals(lA, ledgerA, "Should be the ledger I just marked");
        m1.markLedgerReplicated(lA);

        Future<Long> f = getLedgerToReplicate(m2);
        try {
            f.get(1, TimeUnit.SECONDS);
            fail("Shouldn't be able to find a ledger to replicate");
        } catch (TimeoutException te) {
            // correct behaviour
        }
    }

    /**
     * Test that multiple LedgerUnderreplicationManagers should be able to take
     * lock and release for same ledger.
     */
    @Test(timeOut = 240000, dataProvider = "impl")
    public void testMultipleManagersShouldBeAbleToTakeAndReleaseLock(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);
        String missingReplica1 = "localhost:3181";
        final LedgerUnderreplicationManager m1 = lmf
                .newLedgerUnderreplicationManager();
        final LedgerUnderreplicationManager m2 = lmf
                .newLedgerUnderreplicationManager();
        Long ledgerA = 0xfeadeefdacL;
        m1.markLedgerUnderreplicated(ledgerA, missingReplica1);
        final int iterationCount = 100;
        final CountDownLatch latch1 = new CountDownLatch(iterationCount);
        final CountDownLatch latch2 = new CountDownLatch(iterationCount);
        Thread thread1 = new Thread(() -> takeLedgerAndRelease(m1, latch1, iterationCount));

        Thread thread2 = new Thread(() -> takeLedgerAndRelease(m2, latch2, iterationCount));
        thread1.start();
        thread2.start();

        // wait until at least one thread completed
        while (!latch1.await(50, TimeUnit.MILLISECONDS)
                && !latch2.await(50, TimeUnit.MILLISECONDS)) {
            Thread.sleep(50);
        }

        m1.close();
        m2.close();

        // After completing 'lock acquire,release' job, it should notify below
        // wait
        latch1.await();
        latch2.await();
    }

    /**
     * Test verifies failures of bookies which are resembling each other.
     *
     * <p>BK servers named like*********************************************
     * 1.cluster.com, 2.cluster.com, 11.cluster.com, 12.cluster.com
     * *******************************************************************
     *
     * <p>BKserver IP:HOST like*********************************************
     * localhost:3181, localhost:318, localhost:31812
     * *******************************************************************
     */
    @Test(timeOut = 60000, dataProvider = "impl")
    public void testMarkSimilarMissingReplica(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);
        List<String> missingReplica = new ArrayList<String>();
        missingReplica.add("localhost:3181");
        missingReplica.add("localhost:318");
        missingReplica.add("localhost:31812");
        missingReplica.add("1.cluster.com");
        missingReplica.add("2.cluster.com");
        missingReplica.add("11.cluster.com");
        missingReplica.add("12.cluster.com");
        verifyMarkLedgerUnderreplicated(missingReplica);
    }

    /**
     * Test multiple bookie failures for a ledger and marked as underreplicated
     * one after another.
     */
    @Test(timeOut = 60000, dataProvider = "impl")
    public void testManyFailuresInAnEnsemble(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);
        List<String> missingReplica = new ArrayList<String>();
        missingReplica.add("localhost:3181");
        missingReplica.add("localhost:3182");
        verifyMarkLedgerUnderreplicated(missingReplica);
    }

    /**
     * Test disabling the ledger re-replication. After disabling, it will not be
     * able to getLedgerToRereplicate(). This calls will enter into infinite
     * waiting until enabling rereplication process
     */
    @Test(timeOut = 60000, dataProvider = "impl")
    public void testDisableLedgerReplication(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);
        // simulate few urLedgers before disabling
        final Long ledgerA = 0xfeadeefdacL;
        final String missingReplica = "localhost:3181";

        // disabling replication
        AtomicInteger callbackCount = new AtomicInteger();
        lum.notifyLedgerReplicationEnabled((rc, result) -> callbackCount.incrementAndGet());
        lum.disableLedgerReplication();
        log.info("Disabled Ledeger Replication");

        try {
            lum.markLedgerUnderreplicated(ledgerA, missingReplica);
        } catch (UnavailableException e) {
            log.error().exception(e).log("Unexpected exception while marking urLedger");
            fail("Unexpected exception while marking urLedger" + e.getMessage());
        }

        Future<Long> fA = getLedgerToReplicate(lum);
        try {
            fA.get(1, TimeUnit.SECONDS);
            fail("Shouldn't be able to find a ledger to replicate");
        } catch (TimeoutException te) {
            // expected behaviour, as the replication is disabled
        }
        assertEquals(callbackCount.get(), 1, "Notify callback times mismatch");
    }

    /**
     * Test enabling the ledger re-replication. After enableLedegerReplication,
     * should continue getLedgerToRereplicate() task
     */
    @Test(timeOut = 60000, dataProvider = "impl")
    public void testEnableLedgerReplication(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);

        // simulate few urLedgers before disabling
        final Long ledgerA = 0xfeadeefdacL;
        final String missingReplica = "localhost:3181";
        try {
            lum.markLedgerUnderreplicated(ledgerA, missingReplica);
        } catch (UnavailableException e) {
            log.debug().exception(e).log("Unexpected exception while marking urLedger");
            fail("Unexpected exception while marking urLedger" + e.getMessage());
        }
        AtomicInteger callbackCount = new AtomicInteger();
        lum.notifyLedgerReplicationEnabled((rc, result) -> callbackCount.incrementAndGet());
        // disabling replication
        lum.disableLedgerReplication();
        log.debug("Disabled Ledeger Replication");

        String znodeA = getUrLedgerZnode(ledgerA);
        final CountDownLatch znodeLatch = new CountDownLatch(2);
        String urledgerA = StringUtils.substringAfterLast(znodeA, "/");
        String urLockLedgerA = basePath + "/locks/" + urledgerA;
        store.registerListener(n -> {
            if (n.getType() == NotificationType.Created && n.getPath().equals(urLockLedgerA)) {
                znodeLatch.countDown();
                log.debug("Recieved node creation event for the zNodePath:" + n.getPath());
            }
        });

        // getLedgerToRereplicate is waiting until enable rereplication
        Thread thread1 = new Thread(() -> {
            try {
                Long lA = lum.getLedgerToRereplicate();
                assertEquals(lA, ledgerA, "Should be the ledger I just marked");
                znodeLatch.countDown();
            } catch (UnavailableException e) {
                e.printStackTrace();
            }
        });
        thread1.start();

        try {
            assertFalse(znodeLatch.await(1, TimeUnit.SECONDS), "shouldn't complete");
            assertEquals(znodeLatch.getCount(), 2, "Failed to disable ledger replication!");

            lum.enableLedgerReplication();
            znodeLatch.await(5, TimeUnit.SECONDS);
            log.debug("Enabled Ledeger Replication");
            assertEquals(znodeLatch.getCount(), 0, "Failed to disable ledger replication!");
            assertEquals(callbackCount.get(), 2, "Notify callback times mismatch");
        } finally {
            thread1.interrupt();
        }
    }

    @Test(timeOut = 60000, dataProvider = "impl")
    public void testCheckAllLedgersCTime(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);
        @Cleanup
        LedgerUnderreplicationManager underReplicaMgr1 = lmf.newLedgerUnderreplicationManager();
        @Cleanup
        LedgerUnderreplicationManager underReplicaMgr2 = lmf.newLedgerUnderreplicationManager();
        assertEquals(underReplicaMgr1.getCheckAllLedgersCTime(), -1);
        long curTime = System.currentTimeMillis();
        underReplicaMgr2.setCheckAllLedgersCTime(curTime);
        assertEquals(underReplicaMgr1.getCheckAllLedgersCTime(), curTime);
        curTime = System.currentTimeMillis();
        underReplicaMgr2.setCheckAllLedgersCTime(curTime);
        assertEquals(underReplicaMgr1.getCheckAllLedgersCTime(), curTime);
    }

    @Test(timeOut = 60000, dataProvider = "impl")
    public void testPlacementPolicyCheckCTime(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);

        @Cleanup
        LedgerUnderreplicationManager underReplicaMgr1 = lmf.newLedgerUnderreplicationManager();
        @Cleanup
        LedgerUnderreplicationManager underReplicaMgr2 = lmf.newLedgerUnderreplicationManager();

        assertEquals(underReplicaMgr1.getPlacementPolicyCheckCTime(), -1);
        long curTime = System.currentTimeMillis();
        underReplicaMgr2.setPlacementPolicyCheckCTime(curTime);

        assertEquals(underReplicaMgr1.getPlacementPolicyCheckCTime(), curTime);
        curTime = System.currentTimeMillis();
        underReplicaMgr2.setPlacementPolicyCheckCTime(curTime);

        assertEquals(underReplicaMgr1.getPlacementPolicyCheckCTime(), curTime);
    }

    @Test(timeOut = 60000, dataProvider = "impl")
    public void testReplicasCheckCTime(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);

        @Cleanup
        LedgerUnderreplicationManager underReplicaMgr1 = lmf.newLedgerUnderreplicationManager();
        @Cleanup
        LedgerUnderreplicationManager underReplicaMgr2 = lmf.newLedgerUnderreplicationManager();
        assertEquals(underReplicaMgr1.getReplicasCheckCTime(), -1);
        long curTime = System.currentTimeMillis();
        underReplicaMgr2.setReplicasCheckCTime(curTime);
        assertEquals(underReplicaMgr1.getReplicasCheckCTime(), curTime);
        curTime = System.currentTimeMillis();
        underReplicaMgr2.setReplicasCheckCTime(curTime);
        assertEquals(underReplicaMgr1.getReplicasCheckCTime(), curTime);
    }

    @Test(timeOut = 60000, dataProvider = "impl")
    public void testLostBookieRecoveryDelay(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);

        AtomicInteger callbackCount = new AtomicInteger();
        lum.notifyLostBookieRecoveryDelayChanged((rc, result) -> callbackCount.incrementAndGet());
        // disabling replication
        lum.setLostBookieRecoveryDelay(10);
        Awaitility.await().until(() -> callbackCount.get() == 2);
    }

    private void verifyMarkLedgerUnderreplicated(Collection<String> missingReplica) throws Exception {
        Long ledgerA = 0xfeadeefdacL;
        String znodeA = getUrLedgerZnode(ledgerA);
        for (String replica : missingReplica) {
            lum.markLedgerUnderreplicated(ledgerA, replica);
        }

        String urLedgerA = new String(store.get(znodeA).join().get().getValue());
        UnderreplicatedLedgerFormat builderA = new UnderreplicatedLedgerFormat();
        for (String replica : missingReplica) {
            builderA.addReplica(replica);
        }
        List<String> replicaList = builderA.getReplicasList();

        for (String replica : missingReplica) {
            assertTrue(replicaList.contains(replica),
                    "UrLedger:" + urLedgerA + " doesn't contain failed bookie :" + replica);
        }
    }

    private String getUrLedgerZnode(long ledgerId) {
        return ZkLedgerUnderreplicationManager.getUrLedgerZnode(urLedgerPath, ledgerId);
    }

    private void takeLedgerAndRelease(final LedgerUnderreplicationManager m,
                                      final CountDownLatch latch, int numberOfIterations) {
        for (int i = 0; i < numberOfIterations; i++) {
            try {
                long ledgerToRereplicate = m.getLedgerToRereplicate();
                m.releaseUnderreplicatedLedger(ledgerToRereplicate);
            } catch (UnavailableException e) {
                log.error().exception(e).log("UnavailableException when taking or releasing lock");
            }
            latch.countDown();
        }
    }
}
