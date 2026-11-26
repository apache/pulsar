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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.MetadataCompressionConfig;
import org.apache.bookkeeper.mledger.impl.MetaStore.CursorPublicationOutcomeUnknownException;
import org.apache.bookkeeper.mledger.impl.MetaStore.CursorWrite;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.StringProperty;
import org.apache.bookkeeper.mledger.util.Futures.CloseFuture;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.MultiCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.proto.SetDataRequest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class MetaStoreImplCursorPublicationTest extends MockedBookKeeperTestCase {
    private static final String PATH = "/managed-ledgers/ledger/cursor";

    private static ManagedCursorInfo publication(long ledgerId) {
        return ManagedCursorInfo.newBuilder().setCursorsLedgerId(ledgerId)
                .setPublication(UUID.randomUUID().toString()).build();
    }

    private static CompletableFuture<Stat> write(MetaStore store, ManagedCursorInfo info, Stat expected) {
        return write(store, info, expected, CursorWrite.UPDATE);
    }

    private static CompletableFuture<Stat> write(MetaStore store, ManagedCursorInfo info, Stat expected,
                                                  CursorWrite write) {
        var result = new CompletableFuture<Stat>();
        store.asyncUpdateCursorInfo("ledger", "cursor", info, expected, null, write, new MetaStoreCallback<>() {
            @Override
            public void operationComplete(Void ignored, Stat stat) {
                result.complete(stat);
            }

            @Override
            public void operationFailed(MetaStoreException error) {
                result.completeExceptionally(error);
            }
        });
        return result;
    }

    private static void loseNextMultiResponse(ZooKeeper client, ZooKeeper server) {
        doAnswer(invocation -> {
            Iterable<Op> ops = invocation.getArgument(0);
            MultiCallback callback = invocation.getArgument(1);
            server.multi(ops, (rc, path, ctx, results) ->
                    callback.processResult(rc == Code.OK.intValue() ? Code.CONNECTIONLOSS.intValue() : rc,
                            path, ctx, null), invocation.getArgument(2));
            return null;
        }).doAnswer(delegatesTo(server)).when(client).multi(any(), any(MultiCallback.class), any());
    }

    @Test(timeOut = 30_000)
    public void committedCreateAndUpdateRecoverAfterLostReplies() throws Exception {
        try (var server = MockZooKeeper.newInstance()) {
            ZooKeeper client = mock(ZooKeeper.class, delegatesTo(server));
            try (var zkStore = new ZKMetadataStore(client)) {
                var store = new MetaStoreImpl(zkStore, executor);
                // Ensure the write can commit in its first multi, before dropping that successful response.
                zkStore.put("/managed-ledgers/ledger", new byte[0], Optional.of(-1L)).get();
                // Also exercise legacy callers: a missing ID is generated once for the internal attempt.
                var create = ManagedCursorInfo.newBuilder().setCursorsLedgerId(1).build();
                loseNextMultiResponse(client, server);
                Stat created = write(store, create, null).get(5, TimeUnit.SECONDS);
                assertThat(created.getVersion()).isZero();
                var persisted = ManagedCursorInfo.parseFrom(server.getData(PATH, false, null));
                assertThat(persisted.getPublication()).isNotEmpty();
                assertThat(create.getPublication()).isEmpty();
                assertThat(write(store, persisted, null).get(5, TimeUnit.SECONDS).getVersion()).isZero();

                var update = publication(2);
                loseNextMultiResponse(client, server);
                Stat updated = write(store, update, created).get(5, TimeUnit.SECONDS);
                assertThat(updated.getVersion()).isOne();
                assertThat(server.getData(PATH, false, null)).isEqualTo(update.toByteArray());
                assertThat(write(store, update, created).get(5, TimeUnit.SECONDS)).isEqualTo(updated);
                assertThat(updated).isEqualTo(zkStore.get(PATH).get().orElseThrow().getStat());
            }
        }
    }

    @Test(timeOut = 30_000)
    public void confirmedCreateInvalidatesCachesWhenWatchWasLost() throws Exception {
        try (var server = MockZooKeeper.newInstance()) {
            ZooKeeper client = mock(ZooKeeper.class, delegatesTo(server));
            doAnswer(inv -> null).when(client)
                    .addWatch(eq("/"), any(Watcher.class), eq(AddWatchMode.PERSISTENT_RECURSIVE));
            try (var zkStore = new ZKMetadataStore(client)) {
                server.create("/managed-ledgers", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                server.create("/managed-ledgers/ledger", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
                assertThat(zkStore.exists(PATH).get()).isFalse();
                assertThat(zkStore.getChildren("/managed-ledgers/ledger").get()).isEmpty();
                loseNextMultiResponse(client, server);
                write(new MetaStoreImpl(zkStore, executor), publication(1), null).get(5, TimeUnit.SECONDS);
                assertThat(zkStore.exists(PATH).get()).isTrue();
                assertThat(zkStore.getChildren("/managed-ledgers/ledger").get()).containsExactly("cursor");
            }
        }
    }

    @Test(timeOut = 30_000)
    public void conflictingPublicationsAreNotAccepted() throws Exception {
        try (var server = MockZooKeeper.newInstance(); var zkStore = new ZKMetadataStore(server)) {
            server.setSessionId(1);
            var store = new MetaStoreImpl(zkStore, executor);
            var initial = publication(1);
            Stat created = write(store, initial, null).get(5, TimeUnit.SECONDS);
            var update = publication(2);
            write(store, update, created).get(5, TimeUnit.SECONDS);
            // Same business fields with a different ID are a different write.
            assertThatThrownBy(() -> write(store, publication(2), created).get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerException.BadVersionException.class);
            // An ID alone cannot confirm changed data.
            assertThatThrownBy(() -> write(store, update.toBuilder().setCursorsLedgerId(3).build(), created)
                    .get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(ManagedLedgerException.BadVersionException.class);
            zkStore.put(PATH, update.toByteArray(), Optional.of(1L)).get();
            assertThatThrownBy(() -> write(store, update, created).get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerException.BadVersionException.class);
            zkStore.delete(PATH, Optional.empty()).get();
            assertThatThrownBy(() -> write(store, update, created).get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerException.BadVersionException.class);
            server.create(PATH, initial.toByteArray(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            assertThatThrownBy(() -> write(store, initial, null).get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerException.BadVersionException.class);
        }
    }

    @DataProvider
    public Object[][] transientCodes() {
        return new Object[][]{{Code.CONNECTIONLOSS}, {Code.OPERATIONTIMEOUT}, {Code.SESSIONEXPIRED},
                {Code.SESSIONMOVED}, {Code.REQUESTTIMEOUT}, {Code.THROTTLEDOP}};
    }

    @Test(dataProvider = "transientCodes", timeOut = 30_000)
    public void retriesTransientVerificationErrors(Code failure) throws Exception {
        try (var server = MockZooKeeper.newInstance()) {
            ZooKeeper client = mock(ZooKeeper.class, delegatesTo(server));
            try (var zkStore = new ZKMetadataStore(client)) {
                var store = new MetaStoreImpl(zkStore, executor);
                var info = publication(1);
                write(store, info, null).get(5, TimeUnit.SECONDS);
                var reads = new AtomicInteger();
                doAnswer(inv -> {
                    DataCallback callback = inv.getArgument(2);
                    if (reads.incrementAndGet() <= 2) {
                        callback.processResult(failure.intValue(), PATH, null, null, null);
                    } else {
                        server.getData(PATH, false, callback, inv.getArgument(3));
                    }
                    return null;
                }).when(client).getData(eq(PATH), eq(false), any(DataCallback.class), any());
                assertThat(write(store, info, null).get(5, TimeUnit.SECONDS).getVersion()).isZero();
                assertThat(reads.get()).isEqualTo(3);
                assertThat(server.exists(PATH, false).getVersion()).isZero();
            }
        }
    }

    @Test(timeOut = 30_000)
    public void keepsWriteAndPermanentReadFailures() throws Exception {
        try (var server = MockZooKeeper.newInstance()) {
            ZooKeeper client = mock(ZooKeeper.class, delegatesTo(server));
            try (var zkStore = new ZKMetadataStore(client)) {
                var store = new MetaStoreImpl(zkStore, executor);
                var info = publication(1);
                doAnswer(inv -> {
                    MultiCallback callback = inv.getArgument(1);
                    callback.processResult(Code.OPERATIONTIMEOUT.intValue(), PATH, null, null);
                    return null;
                }).doAnswer(delegatesTo(server)).when(client).multi(any(), any(MultiCallback.class), any());
                assertThatThrownBy(() -> write(store, info, null).get(5, TimeUnit.SECONDS))
                        .hasRootCauseInstanceOf(KeeperException.OperationTimeoutException.class);
                write(store, info, null).get(5, TimeUnit.SECONDS);
                doAnswer(inv -> {
                    DataCallback callback = inv.getArgument(2);
                    callback.processResult(Code.NOAUTH.intValue(), PATH, null, null, null);
                    return null;
                }).when(client).getData(eq(PATH), eq(false), any(DataCallback.class), any());
                assertThatThrownBy(() -> write(store, info, null).get(5, TimeUnit.SECONDS))
                        .hasRootCauseInstanceOf(KeeperException.NoAuthException.class);
            }
        }
    }

    @Test(timeOut = 30_000)
    public void factoryShutdownFailsPendingVerificationWithoutClosingSharedStore() throws Exception {
        try (var server = MockZooKeeper.newInstance()) {
            ZooKeeper client = mock(ZooKeeper.class, delegatesTo(server));
            try (var zkStore = new ZKMetadataStore(client)) {
                var zkFactory = new ManagedLedgerFactoryImpl(zkStore, bkc);
                try {
                    MetaStore store = zkFactory.getMetaStore();
                    var info = publication(1);
                    write(store, info, null).get(5, TimeUnit.SECONDS);
                    var reading = new CompletableFuture<Void>();
                    doAnswer(inv -> {
                        reading.complete(null);
                        return null;
                    }).when(client).getData(eq(PATH), eq(false), any(DataCallback.class), any());
                    var pending = write(store, info, null);
                    reading.get(5, TimeUnit.SECONDS);
                    zkFactory.shutdownAsync().get(5, TimeUnit.SECONDS);
                    assertThatThrownBy(() -> pending.get(5, TimeUnit.SECONDS))
                            .hasRootCauseInstanceOf(MetadataStoreException.AlreadyClosedException.class);
                    assertThatThrownBy(() -> write(store, publication(2), null).get(5, TimeUnit.SECONDS))
                            .hasRootCauseInstanceOf(MetadataStoreException.AlreadyClosedException.class);
                    assertThat(zkStore.exists(PATH).get(5, TimeUnit.SECONDS)).isTrue();
                } finally {
                    try {
                        zkFactory.shutdown();
                    } catch (ManagedLedgerException.ManagedLedgerFactoryClosedException alreadyClosed) {
                        // The test shuts down the factory before checking its pending callback.
                    }
                }
            }
        }
    }

    private static CompletableFuture<Void> rollCursorLedger(ManagedCursorImpl cursor) {
        var result = new CompletableFuture<Void>();
        cursor.createNewMetadataLedger(new ManagedCursorImpl.VoidCallback() {
            @Override
            public void operationComplete() {
                result.complete(null);
            }

            @Override
            public void operationFailed(ManagedLedgerException error) {
                result.completeExceptionally(error);
            }
        });
        return result;
    }

    @Test(timeOut = 30_000)
    public void shutdownDuringRolloverPreservesLedgerAndIndividualAck() throws Exception {
        var cluster = new ZooKeeperUtil("/ledgers");
        try {
            cluster.startCluster();
            ZooKeeper server = cluster.getZooKeeperClient();
            ZooKeeper client = mock(ZooKeeper.class, delegatesTo(server));
            try (var zkStore = new ZKMetadataStore(client)) {
                var config = new ManagedLedgerConfig().setMaxUnackedRangesToPersistInMetadataStore(0);
                var first = new ManagedLedgerFactoryImpl(zkStore, bkc);
                long candidate;
                try {
                    var ledger = first.open("ledger", config);
                    var cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
                    ledger.addEntry(new byte[]{1});
                    var individualAck = ledger.addEntry(new byte[]{2});
                    ledger.addEntry(new byte[]{3});
                    cursor.delete(individualAck);
                    long oldLedger = cursor.getCursorLedger();
                    assertThat(oldLedger).isNotNegative();

                    var verifying = new CompletableFuture<Void>();
                    doAnswer(inv -> {
                        verifying.complete(null);
                        return null; // Hold this verification until the factory shuts down.
                    }).doAnswer(delegatesTo(server)).when(client)
                            .getData(eq(PATH), eq(false), any(DataCallback.class), any());
                    loseNextMultiResponse(client, server);
                    var rollover = rollCursorLedger(cursor);
                    verifying.get(5, TimeUnit.SECONDS);
                    var published = ManagedCursorInfo.parseFrom(server.getData(PATH, false, null));
                    candidate = published.getCursorsLedgerId();
                    assertThat(candidate).isNotNegative().isNotEqualTo(oldLedger);
                    assertThat(published.getIndividualDeletedMessagesCount()).isZero();
                    assertThat(cursor.getCursorLedger()).isEqualTo(oldLedger);
                    assertThat(rollover).isNotDone();

                    first.shutdownAsync().get(5, TimeUnit.SECONDS);
                    assertThatThrownBy(() -> rollover.get(5, TimeUnit.SECONDS))
                            .hasRootCauseInstanceOf(MetadataStoreException.AlreadyClosedException.class);
                    assertThat(ManagedCursorInfo.parseFrom(server.getData(PATH, false, null)).getCursorsLedgerId())
                            .isEqualTo(candidate);
                } finally {
                    try {
                        first.shutdown();
                    } catch (ManagedLedgerException.ManagedLedgerFactoryClosedException alreadyClosed) {
                        // The factory was closed while the verification was pending.
                    }
                }

                boolean ledgerSurvived = bkc.getLedgers().contains(candidate);
                var reopened = new ManagedLedgerFactoryImpl(zkStore, bkc);
                try {
                    var cursor = reopened.open("ledger", config).openCursor("cursor");
                    var entries = cursor.readEntries(3);
                    try {
                        assertThat(entries).extracting(entry -> (int) entry.getData()[0]).containsExactly(1, 3);
                        assertThat(ledgerSurvived).as("ZooKeeper-referenced ledger %s", candidate).isTrue();
                    } finally {
                        entries.forEach(Entry::release);
                    }
                } finally {
                    reopened.shutdown();
                }
            }
        } finally {
            cluster.killCluster();
        }
    }

    private static void rejectCursorWrites(ZooKeeper zk) throws Exception {
        // Leave READ and ADMIN available so verification still runs and the test can restore WRITE.
        var acl = new ACL(ZooDefs.Perms.READ | ZooDefs.Perms.ADMIN, ZooDefs.Ids.ANYONE_ID_UNSAFE);
        zk.setACL(PATH, Collections.singletonList(acl), -1);
    }

    @DataProvider
    public Object[][] switchRejections() {
        return new Object[][]{{Code.NOAUTH}, {Code.OPERATIONTIMEOUT}};
    }

    @Test(dataProvider = "switchRejections", timeOut = 45_000)
    public void repeatedFailedAcksReuseOneCandidateAfterRecovery(Code rejection) throws Exception {
        var cluster = new ZooKeeperUtil("/ledgers");
        try {
            cluster.startCluster();
            var zk = cluster.getZooKeeperClient();
            ZooKeeper client = mock(ZooKeeper.class, delegatesTo(zk));
            var requests = new LinkedBlockingQueue<SetDataRequest>();
            var reject = new AtomicBoolean(true);
            doAnswer(inv -> {
                Iterable<Op> operations = inv.getArgument(0);
                for (Op op : operations) {
                    if (PATH.equals(op.getPath())
                            && op.toRequestRecord() instanceof SetDataRequest request) {
                        requests.add(request);
                        if (rejection == Code.OPERATIONTIMEOUT && reject.get()) {
                            MultiCallback cb = inv.getArgument(1);
                            cb.processResult(rejection.intValue(), PATH, inv.getArgument(2), null);
                            return null;
                        }
                    }
                }
                return delegatesTo(zk).answer(inv);
            }).when(client).multi(any(), any(MultiCallback.class), any());
            try (var store = new ZKMetadataStore(client)) {
                var writer = new ManagedLedgerFactoryImpl(store, bkc);
                try {
                    var ledger = writer.open("ledger");
                    var cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
                    var first = ledger.addEntry(new byte[]{1});
                    var second = ledger.addEntry(new byte[]{2});
                    var third = ledger.addEntry(new byte[]{3});
                    Set<Long> before = Set.copyOf(bkc.getLedgers());
                    if (rejection == Code.NOAUTH) {
                        rejectCursorWrites(zk);
                    }
                    Class<? extends KeeperException> expectedError = KeeperException.create(rejection).getClass();
                    assertThatThrownBy(() -> cursor.markDelete(first))
                            .hasRootCauseInstanceOf(expectedError);
                    var allocated = new HashSet<>(bkc.getLedgers());
                    allocated.removeAll(before);
                    assertThat(allocated).hasSize(1);
                    long candidate = allocated.iterator().next();
                    assertThatThrownBy(() -> cursor.markDelete(second))
                            .hasRootCauseInstanceOf(expectedError);
                    var afterRetry = new HashSet<>(bkc.getLedgers());
                    afterRetry.removeAll(before);
                    assertThat(afterRetry).containsExactly(candidate);

                    zk.setACL(PATH, ZooDefs.Ids.OPEN_ACL_UNSAFE, -1);
                    reject.set(false);
                    cursor.markDelete(third);
                    assertThat(cursor.getCursorLedger()).isEqualTo(candidate);
                    assertThat(ManagedCursorInfo.parseFrom(zk.getData(PATH, false, null)).getCursorsLedgerId())
                            .isEqualTo(candidate);
                    assertThat(cursor.getPersistentMarkDeletedPosition()).isEqualTo(third);
                    assertThat(requests).hasSize(3);
                    var original = requests.remove();
                    for (var retry : requests) {
                        assertThat(retry.getVersion()).isEqualTo(original.getVersion());
                        assertThat(retry.getData()).isEqualTo(original.getData());
                    }
                } finally {
                    zk.setACL(PATH, ZooDefs.Ids.OPEN_ACL_UNSAFE, -1);
                    writer.shutdown();
                }
            }
        } finally {
            cluster.killCluster();
        }
    }

    @Test(timeOut = 45_000)
    public void unpublishedCandidateCannotAcknowledgeMessagesBeforeRecovery() throws Exception {
        var cluster = new ZooKeeperUtil("/ledgers");
        try {
            cluster.startCluster();
            var zk = cluster.getZooKeeperClient();
            try (var store = new ZKMetadataStore(zk)) {
                var writer = new ManagedLedgerFactoryImpl(store, bkc);
                try {
                    var config = new ManagedLedgerConfig().setMaxUnackedRangesToPersistInMetadataStore(0);
                    var ledger = writer.open("ledger", config);
                    var cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
                    ledger.addEntry(new byte[]{1});
                    var acknowledged = ledger.addEntry(new byte[]{2});
                    var unconfirmed = ledger.addEntry(new byte[]{3});
                    cursor.delete(acknowledged);
                    long oldLedger = cursor.getCursorLedger();
                    var before = Set.copyOf(bkc.getLedgers());
                    rejectCursorWrites(zk);
                    cursor.startCreatingNewMetadataLedger();
                    await().untilAsserted(() -> assertThat(cursor.getState()).isEqualTo("NoLedger"));
                    var afterSwitch = Set.copyOf(bkc.getLedgers());
                    assertThat(afterSwitch).hasSize(before.size() + 1);
                    assertThatThrownBy(() -> cursor.delete(unconfirmed))
                            .hasRootCauseInstanceOf(KeeperException.NoAuthException.class);
                    // A duplicate ACK may already be reflected in memory. It must still wait for publication.
                    assertThatThrownBy(() -> cursor.delete(unconfirmed))
                            .hasRootCauseInstanceOf(KeeperException.NoAuthException.class);
                    assertThat(ManagedCursorInfo.parseFrom(zk.getData(PATH, false, null)).getCursorsLedgerId())
                            .isEqualTo(oldLedger);

                    var afterAck = Set.copyOf(bkc.getLedgers());
                    // Recover from durable state before the original factory can flush its in-memory ACKs on close.
                    zk.setACL(PATH, ZooDefs.Ids.OPEN_ACL_UNSAFE, -1);
                    var recovered = new ManagedLedgerFactoryImpl(store, bkc);
                    try {
                        var reader = recovered.open("ledger").openCursor("cursor");
                        var entries = reader.readEntries(3);
                        try {
                            assertThat(entries).extracting(entry -> (int) entry.getData()[0]).containsExactly(1, 3);
                        } finally {
                            entries.forEach(Entry::release);
                        }
                        assertThat(afterAck).isEqualTo(afterSwitch);
                        // The outstanding ranges require BookKeeper. Neither ledger alone can confirm a final write.
                        assertThatThrownBy(cursor::close).hasMessageContaining("Unverified cursor ledger switch");
                    } finally {
                        zk.setACL(PATH, ZooDefs.Ids.OPEN_ACL_UNSAFE, -1);
                        recovered.shutdown();
                    }
                } finally {
                    zk.setACL(PATH, ZooDefs.Ids.OPEN_ACL_UNSAFE, -1);
                    writer.shutdown();
                }
            }
        } finally {
            cluster.killCluster();
        }
    }

    @Test(timeOut = 45_000)
    public void resolveCandidateFromLaterCursorMetadata() throws Exception {
        var cluster = new ZooKeeperUtil("/ledgers");
        try {
            cluster.startCluster();
            var zk = cluster.getZooKeeperClient();
            ZooKeeper client = mock(ZooKeeper.class, delegatesTo(zk));
            try (var store = new ZKMetadataStore(client)) {
                var writer = new ManagedLedgerFactoryImpl(store, bkc);
                try {
                    var ledger = writer.open("ledger");
                    var cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
                    var first = ledger.addEntry(new byte[]{1});
                    var second = ledger.addEntry(new byte[]{2});
                    var third = ledger.addEntry(new byte[]{3});
                    var before = Set.copyOf(bkc.getLedgers());

                    // The switch committed, but the client sees only a rejection after an internal retry.
                    doAnswer(inv -> {
                        MultiCallback callback = inv.getArgument(1);
                        zk.multi(inv.getArgument(0), (rc, path, ctx, results) ->
                                callback.processResult(rc == Code.OK.intValue() ? Code.NOAUTH.intValue() : rc,
                                        path, ctx, null), inv.getArgument(2));
                        return null;
                    }).doAnswer(delegatesTo(zk)).when(client).multi(any(), any(MultiCallback.class), any());
                    assertThatThrownBy(() -> cursor.markDelete(first))
                            .hasRootCauseInstanceOf(KeeperException.NoAuthException.class);
                    long committed = ManagedCursorInfo.parseFrom(zk.getData(PATH, false, null)).getCursorsLedgerId();
                    assertThat(committed).isNotNegative();
                    var allocated = Set.copyOf(bkc.getLedgers());
                    assertThat(allocated).hasSize(before.size() + 1);
                    cursor.markDelete(second);
                    assertThat(cursor.getCursorLedger()).isEqualTo(committed);
                    assertThat(bkc.getLedgers()).containsExactlyInAnyOrderElementsOf(allocated);

                    // Now deny a fresh switch. A successful property write with the old pointer fences it.
                    rejectCursorWrites(zk);
                    cursor.startCreatingNewMetadataLedger();
                    await().untilAsserted(() -> assertThat(cursor.getState()).isEqualTo("NoLedger"));
                    assertThat(bkc.getLedgers()).hasSize(allocated.size() + 1);
                    zk.setACL(PATH, ZooDefs.Ids.OPEN_ACL_UNSAFE, -1);
                    cursor.setCursorProperties(Map.of("test", "updated")).get(5, TimeUnit.SECONDS);
                    await().untilAsserted(() -> assertThat(bkc.getLedgers())
                            .containsExactlyInAnyOrderElementsOf(allocated));
                    cursor.markDelete(third);
                    assertThat(cursor.getPersistentMarkDeletedPosition()).isEqualTo(third);
                } finally {
                    zk.setACL(PATH, ZooDefs.Ids.OPEN_ACL_UNSAFE, -1);
                    writer.shutdown();
                }
            }
        } finally {
            cluster.killCluster();
        }
    }

    @Test(timeOut = 30_000)
    public void rolloverCleanupRequiresALaterVersionReferencingADifferentLedger() throws Exception {
        var cluster = new ZooKeeperUtil("/ledgers");
        try {
            cluster.startCluster();
            ZooKeeper server = cluster.getZooKeeperClient();
            ZooKeeper client = mock(ZooKeeper.class, delegatesTo(server));
            try (var zkStore = new ZKMetadataStore(client)) {
                var store = new MetaStoreImpl(zkStore, executor);
                write(store, publication(1), null).get(5, TimeUnit.SECONDS);
                Stat expected = zkStore.get(PATH).get(5, TimeUnit.SECONDS).orElseThrow().getStat();
                var candidate = publication(2);
                doAnswer(inv -> {
                    MultiCallback callback = inv.getArgument(1);
                    callback.processResult(Code.OPERATIONTIMEOUT.intValue(), PATH, null, null);
                    return null;
                }).doAnswer(delegatesTo(server)).when(client).multi(any(), any(MultiCallback.class), any());

                // An unchanged version still allows the timed-out CAS to arrive after the verification read.
                assertThatThrownBy(() -> write(store, candidate, expected, CursorWrite.SWITCH_LEDGER)
                        .get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(CursorPublicationOutcomeUnknownException.class);

                // Different bytes can still reference the very same ledger.
                write(store, publication(2), expected).get(5, TimeUnit.SECONDS);
                assertThatThrownBy(() -> write(store, candidate, expected, CursorWrite.SWITCH_LEDGER)
                        .get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(CursorPublicationOutcomeUnknownException.class);

                // A later version of the same node now points elsewhere and fences the original request.
                zkStore.put(PATH, publication(3).toByteArray(), Optional.of(expected.getVersion() + 1))
                        .get(5, TimeUnit.SECONDS);
                assertThatThrownBy(() -> write(store, candidate, expected, CursorWrite.SWITCH_LEDGER)
                        .get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(ManagedLedgerException.BadVersionException.class);
            }
        } finally {
            cluster.killCluster();
        }
    }

    @Test(timeOut = 30_000)
    public void rolloverRejectedBeforePutDeletesCandidate() throws Exception {
        var ledger = factory.open("ledger");
        var cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        var before = Set.copyOf(bkc.getLedgers());
        factory.getMetaStore().close();
        assertThatThrownBy(() -> rollCursorLedger(cursor).get(5, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(MetadataStoreException.AlreadyClosedException.class);
        assertThat(bkc.getLedgers()).isEqualTo(before);
    }

    @Test(timeOut = 30_000)
    public void shutdownDeliversQueuedVerificationCallbackOnce() throws Exception {
        try (var server = MockZooKeeper.newInstance(); var zkStore = new ZKMetadataStore(server)) {
            var info = publication(1);
            write(new MetaStoreImpl(zkStore, executor), info, null).get(5, TimeUnit.SECONDS);
            var tasks = new LinkedBlockingQueue<Runnable>();
            var scheduler = mock(OrderedExecutor.class);
            doAnswer(inv -> tasks.add(inv.getArgument(1))).when(scheduler)
                    .executeOrdered(eq("ledger"), any(Runnable.class));
            var store = new MetaStoreImpl(zkStore, scheduler);
            var result = new CompletableFuture<Stat>();
            var deliveries = new AtomicInteger();
            store.asyncUpdateCursorInfo("ledger", "cursor", info, null, new MetaStoreCallback<>() {
                @Override
                public void operationComplete(Void ignored, Stat stat) {
                    deliveries.incrementAndGet();
                    result.complete(stat);
                }

                @Override
                public void operationFailed(MetaStoreException error) {
                    deliveries.incrementAndGet();
                    result.completeExceptionally(error);
                }
            });
            Runnable verify = tasks.poll(5, TimeUnit.SECONDS);
            assertThat(verify).isNotNull();
            verify.run();
            // The ordered executor has accepted the verified result but has not delivered it yet.
            Runnable deliver = tasks.poll(5, TimeUnit.SECONDS);
            assertThat(deliver).isNotNull();
            store.close();
            assertThat(result.get(2, TimeUnit.SECONDS).getVersion()).isZero();
            deliver.run();
            assertThat(deliveries.get()).isOne();
        }
    }

    @DataProvider
    public Object[][] verificationReadStalls() {
        return new Object[][]{{false}, {true}};
    }

    @Test(dataProvider = "verificationReadStalls", timeOut = 30_000)
    public void ledgerCloseBoundsCursorVerification(boolean withholdReply) throws Exception {
        try (var server = MockZooKeeper.newInstance()) {
            ZooKeeper client = mock(ZooKeeper.class, delegatesTo(server));
            try (var zkStore = new ZKMetadataStore(client)) {
                var zkFactory = new ManagedLedgerFactoryImpl(zkStore, bkc);
                try {
                    var config = new ManagedLedgerConfig().setMetadataOperationsTimeoutSeconds(1);
                    var ledger = zkFactory.open("ledger", config);
                    ledger.openCursor("cursor");
                    int version = server.exists(PATH, false).getVersion();
                    var reads = new AtomicInteger();
                    doAnswer(inv -> {
                        reads.incrementAndGet();
                        if (!withholdReply) {
                            DataCallback callback = inv.getArgument(2);
                            callback.processResult(Code.OPERATIONTIMEOUT.intValue(), PATH, null, null, null);
                        }
                        return null;
                    }).when(client).getData(eq(PATH), eq(false), any(DataCallback.class), any());
                    loseNextMultiResponse(client, server);
                    var closed = new CloseFuture();
                    ledger.asyncClose(closed, null);
                    assertThatThrownBy(() -> closed.get(5, TimeUnit.SECONDS))
                            .isInstanceOf(ExecutionException.class).rootCause()
                            .isInstanceOf(TimeoutException.class).hasMessageContaining("outcome is unknown");
                    assertThat(server.exists(PATH, false).getVersion()).isEqualTo(version + 1);
                    int atTimeout = reads.get();
                    assertThat(atTimeout).isGreaterThanOrEqualTo(withholdReply ? 1 : 2);
                    // Even a retry scheduled just before the deadline must not start another read.
                    await().during(1200, TimeUnit.MILLISECONDS).atMost(3, TimeUnit.SECONDS)
                            .until(() -> reads.get() == atTimeout);
                } finally {
                    zkFactory.shutdown();
                }
            }
        }
    }

    @Test(timeOut = 30_000)
    public void encodingAndSynchronousPutFailuresReachCallback() throws Exception {
        var info = publication(1);
        var store = spy(new MetaStoreImpl(metadataStore, executor));
        var failure = new IllegalStateException("Failed to encode cursor");
        doThrow(failure).when(store).compressCursorInfo(info);
        assertThatThrownBy(() -> write(store, info, null).get(5, TimeUnit.SECONDS)).hasRootCause(failure);
        var failingStore = spy(metadataStore);
        doThrow(failure).when(failingStore).put(eq(PATH), any(), eq(Optional.of(-1L)));
        assertThatThrownBy(() -> write(new MetaStoreImpl(failingStore, executor), info, null)
                .get(5, TimeUnit.SECONDS)).hasRootCause(failure);
    }

    @Test(timeOut = 30_000)
    public void otherBackendsKeepTheirConflictSemantics() throws Exception {
        var store = new MetaStoreImpl(metadataStore, executor);
        var info = publication(1);
        write(store, info, null).get(5, TimeUnit.SECONDS);
        assertThatThrownBy(() -> write(store, info, null).get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ManagedLedgerException.BadVersionException.class);
    }

    @Test(timeOut = 30_000)
    public void newCursorWritesGenerateNewIds() throws Exception {
        var ledger = factory.open("ledger");
        var cursor = ledger.openCursor("cursor");
        var previous = ManagedCursorInfo.parseFrom(metadataStore.get(PATH).get().orElseThrow().getValue());
        assertThat(previous.getPublication()).isNotEmpty();
        for (int i = 0; i < 2; i++) {
            cursor.setCursorProperties(Map.of("key", "value")).get(5, TimeUnit.SECONDS);
            var current = ManagedCursorInfo.parseFrom(metadataStore.get(PATH).get().orElseThrow().getValue());
            assertThat(current.getPublication()).isNotEmpty().isNotEqualTo(previous.getPublication());
            previous = current;
        }
    }

    @Test(timeOut = 30_000)
    public void verificationReturnsStatFromTheCurrentSession() throws Exception {
        var cluster = new ZooKeeperUtil("/ledgers");
        try {
            cluster.startCluster();
            var config = MetadataStoreConfig.builder().build();
            try (var first = new ZKMetadataStore(cluster.getZooKeeperConnectString(), config, false);
                 var second = new ZKMetadataStore(cluster.getZooKeeperConnectString(), config, false)) {
                var info = publication(1);
                write(new MetaStoreImpl(first, executor), info, null).get(5, TimeUnit.SECONDS);
                assertThat(first.getZkSessionId()).isNotEqualTo(second.getZkSessionId());
                var confirmed = write(new MetaStoreImpl(second, executor), info, null).get(5, TimeUnit.SECONDS);
                assertThat(confirmed).isEqualTo(second.get(PATH).get().orElseThrow().getStat());
                assertThat(confirmed.isCreatedBySelf()).isFalse();
            }
        } finally {
            cluster.killCluster();
        }
    }

    private static class LimitedZooKeeper extends ZooKeeperUtil {
        LimitedZooKeeper() {
            super("/ledgers");
        }

        void limitMultiReadResponse() {
            zks.setMultiReadMaxResponseBytes(64 * 1024);
        }
    }

    @Test(timeOut = 45_000)
    public void confirmsLargeIndividualWritesAfterLostReplies() throws Exception {
        var cluster = new LimitedZooKeeper();
        try {
            cluster.startCluster();
            cluster.limitMultiReadResponse();
            ZooKeeper zk = cluster.getZooKeeperClient();
            ZooKeeper client = mock(ZooKeeper.class, delegatesTo(zk));
            var config = MetadataStoreConfig.builder().batchingMaxOperations(2).batchingMaxDelayMillis(60_000).build();
            // Use the unwrapped client for setup; the test store intentionally buffers pairs of writes.
            zk.create("/existing", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/managed-ledgers", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/managed-ledgers/ledger", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            var committed = new AtomicInteger();
            doAnswer(inv -> {
                StringCallback callback = inv.getArgument(4);
                zk.create(PATH, inv.getArgument(1), inv.getArgument(2), inv.getArgument(3), (rc, p, ctx, name) -> {
                    if (rc == Code.OK.intValue()) {
                        committed.incrementAndGet();
                        rc = Code.CONNECTIONLOSS.intValue();
                    }
                    callback.processResult(rc, p, ctx, null);
                }, null);
                return null;
            }).when(client).create(eq(PATH), any(), any(), any(), any(StringCallback.class), any());
            doAnswer(inv -> {
                StatCallback callback = inv.getArgument(3);
                zk.setData(PATH, inv.getArgument(1), inv.getArgument(2), (rc, p, ctx, stat) -> {
                    if (rc == Code.OK.intValue()) {
                        committed.incrementAndGet();
                        rc = Code.CONNECTIONLOSS.intValue();
                    }
                    callback.processResult(rc, p, ctx, null);
                }, null);
                return null;
            }).when(client).setData(eq(PATH), any(), anyInt(), any(StatCallback.class), any());
            try (var zkStore = new ZKMetadataStore(client, config)) {
                var store = new MetaStoreImpl(zkStore, executor);
                Stat previous = null;
                for (int i = 0; i < 2; i++) {
                    var info = publication(i).toBuilder()
                            .addCursorProperties(StringProperty.newBuilder().setName("payload")
                                    .setValue("x".repeat(96 * 1024)).build())
                            .build();
                    var conflict = zkStore.put("/existing", new byte[0], Optional.of(-1L));
                    var written = write(store, info, previous);
                    zkStore.put("/flush-" + i, new byte[0], Optional.of(-1L)).get(5, TimeUnit.SECONDS);
                    assertThatThrownBy(() -> conflict.get(5, TimeUnit.SECONDS))
                            .hasCauseInstanceOf(MetadataStoreException.BadVersionException.class);
                    previous = written.get(5, TimeUnit.SECONDS);
                    assertThat(previous.getVersion()).isEqualTo(i);
                    assertThat(zk.getData(PATH, false, null)).isEqualTo(info.toByteArray());
                }
                assertThat(committed.get()).isEqualTo(2);
            }
        } finally {
            cluster.killCluster();
        }
    }

    @DataProvider
    public Object[][] compressionTypes() {
        return new Object[][]{{"NONE"}, {"LZ4"}, {"ZLIB"}, {"ZSTD"}, {"SNAPPY"}};
    }

    @Test(dataProvider = "compressionTypes", timeOut = 30_000)
    public void confirmsCompressedPublication(String compressionType) throws Exception {
        try (var server = MockZooKeeper.newInstance()) {
            ZooKeeper client = mock(ZooKeeper.class, delegatesTo(server));
            try (var zkStore = new ZKMetadataStore(client)) {
                var store = new MetaStoreImpl(zkStore, executor, MetadataCompressionConfig.noCompression,
                        new MetadataCompressionConfig(compressionType, 0));
                zkStore.put("/managed-ledgers/ledger", new byte[0], Optional.of(-1L)).get();
                var info = publication(1);
                loseNextMultiResponse(client, server);
                assertThat(write(store, info, null).get(5, TimeUnit.SECONDS).getVersion()).isZero();
                assertThat(store.parseManagedCursorInfo(server.getData(PATH, false, null))).isEqualTo(info);
                assertThat(write(store, info, null).get(5, TimeUnit.SECONDS).getVersion()).isZero();
            }
        }
    }

    @Test(timeOut = 45_000)
    public void openCursorRecoversAfterVerificationTimeouts() throws Exception {
        var cluster = new ZooKeeperUtil("/ledgers");
        try {
            cluster.startCluster();
            ZooKeeper zk = cluster.getZooKeeperClient();
            ZooKeeper client = mock(ZooKeeper.class, delegatesTo(zk));
            try (var zkStore = new ZKMetadataStore(client)) {
                var zkFactory = new ManagedLedgerFactoryImpl(zkStore, bkc);
                try {
                    var ledger = zkFactory.open("ledger");
                    var lostCreate = new AtomicBoolean();
                    doAnswer(inv -> {
                        Iterable<Op> ops = inv.getArgument(0);
                        MultiCallback callback = inv.getArgument(1);
                        for (Op op : ops) {
                            if (op.getType() == OpCode.create && op.getPath().equals(PATH)) {
                                zk.multi(ops, (rc, p, ctx, results) -> {
                                    if (rc == Code.OK.intValue() && lostCreate.compareAndSet(false, true)) {
                                        callback.processResult(Code.CONNECTIONLOSS.intValue(), p, ctx, null);
                                    } else {
                                        callback.processResult(rc, p, ctx, results);
                                    }
                                }, inv.getArgument(2));
                                return null;
                            }
                        }
                        zk.multi(ops, callback, inv.getArgument(2));
                        return null;
                    }).when(client).multi(any(), any(MultiCallback.class), any());
                    var reads = new AtomicInteger();
                    doAnswer(inv -> {
                        DataCallback callback = inv.getArgument(2);
                        if (reads.incrementAndGet() <= 2) {
                            callback.processResult(Code.OPERATIONTIMEOUT.intValue(), PATH, null, null, null);
                        } else {
                            zk.getData(PATH, false, callback, inv.getArgument(3));
                        }
                        return null;
                    }).when(client).getData(eq(PATH), eq(false), any(DataCallback.class), any());
                    var cursor = ledger.openCursor("cursor");
                    assertThat(ledger.openCursor("cursor")).isSameAs(cursor);
                    assertThat(lostCreate.get()).isTrue();
                    assertThat(reads.get()).isEqualTo(3);
                    assertThat(zk.exists(PATH, false).getVersion()).isZero();
                } finally {
                    zkFactory.shutdown();
                }
            }
        } finally {
            cluster.killCluster();
        }
    }
}
