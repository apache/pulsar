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
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.proto.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.MultiCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.ZooKeeper;
import org.testng.annotations.Test;

public class MetaStoreImplTest extends MockedBookKeeperTestCase {

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class MyClass {
        String a;
        int b;
    }

    @Test
    void getMLList() throws Exception {
        MetaStore store = new MetaStoreImpl(metadataStore, executor);

        metadataStore.failConditional(new MetadataStoreException("error"), (op, path) ->
                op == FaultInjectionMetadataStore.OperationType.GET_CHILDREN
                    && path.equals("/managed-ledgers")
            );

        try {
            store.getManagedLedgers();
            fail("should fail in getting the list");
        } catch (MetaStoreException e) {
            // ok
        }
    }

    @Test
    void deleteNonExistingML() throws Exception {
        MetaStore store = new MetaStoreImpl(metadataStore, executor);

        AtomicReference<MetaStoreException> exception = new AtomicReference<>();
        CountDownLatch counter = new CountDownLatch(1);

        store.removeManagedLedger("non-existing", new MetaStoreCallback<Void>() {
            @Override
            public void operationComplete(Void result, Stat version) {
                counter.countDown();
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                exception.set(e);
                counter.countDown();
            }

        });

        counter.await();
        assertNotNull(exception.get());
    }

    @Test(timeOut = 20000)
    void readMalformedML() throws Exception {
        MetaStore store = new MetaStoreImpl(metadataStore, executor);
        metadataStore.put("/managed-ledgers/my_test", "non-valid".getBytes(), Optional.empty()).join();

        final CountDownLatch latch = new CountDownLatch(1);

        store.getManagedLedgerInfo("my_test", false, new MetaStoreCallback<ManagedLedgerInfo>() {
            public void operationFailed(MetaStoreException e) {
                // Ok
                latch.countDown();
            }

            public void operationComplete(ManagedLedgerInfo result, Stat version) {
                fail("Operation should have failed");
            }
        });

        latch.await();
    }

    @Test(timeOut = 20000)
    void readMalformedCursorNode() throws Exception {
        MetaStore store = new MetaStoreImpl(metadataStore, executor);

        metadataStore.put("/managed-ledgers/my_test", "".getBytes(), Optional.empty()).join();
        metadataStore.put("/managed-ledgers/my_test/c1", "non-valid".getBytes(), Optional.empty()).join();

        final CountDownLatch latch = new CountDownLatch(1);
        store.asyncGetCursorInfo("my_test", "c1", new MetaStoreCallback<ManagedCursorInfo>() {

            public void operationFailed(MetaStoreException e) {
                // Ok
                latch.countDown();
            }

            public void operationComplete(ManagedCursorInfo result, Stat version) {
                fail("Operation should have failed");
            }
        });

        latch.await();
    }

    @Test(timeOut = 20000)
    void failInCreatingMLnode() throws Exception {
        MetaStore store = new MetaStoreImpl(metadataStore, executor);

        final CompletableFuture<Void> promise = new CompletableFuture<>();

        metadataStore.failConditional(new MetadataStoreException("error"), (op, path) ->
                op == FaultInjectionMetadataStore.OperationType.PUT
        );

        store.getManagedLedgerInfo("my_test", false, new MetaStoreCallback<ManagedLedgerInfo>() {
            public void operationFailed(MetaStoreException e) {
                promise.complete(null);
            }

            public void operationComplete(ManagedLedgerInfo result, Stat version) {
                promise.completeExceptionally(new Exception("Operation should have failed"));
            }
        });
        promise.get();
    }

    /**
     * The z-node backing a managed ledger can be created by another party (for example
     * {@code AdminResource#tryCreatePartitionAsync}) in between the read and the write that
     * {@code getManagedLedgerInfo(createIfMissing = true)} performs. The metadata store reports that as a
     * {@link MetadataStoreException.BadVersionException}, which must not fail the managed ledger initialization.
     */
    @Test(timeOut = 20000)
    void createMLNodeConcurrently() throws Exception {
        String ledgerName = "my_test";
        String path = "/managed-ledgers/" + ledgerName;

        AtomicBoolean raceInjected = new AtomicBoolean();
        MetadataStoreExtended racyStore = spy(metadataStore);
        doAnswer(invocation -> {
            if (path.equals(invocation.getArgument(0)) && raceInjected.compareAndSet(false, true)) {
                // Create the z-node behind the caller's back and report it as still missing, so that the
                // subsequent create hits an already existing z-node.
                metadataStore.put(path, new byte[0], Optional.of(-1L)).get();
                return CompletableFuture.completedFuture(Optional.empty());
            }
            return invocation.callRealMethod();
        }).when(racyStore).get(anyString(), any());

        MetaStore store = new MetaStoreImpl(racyStore, executor);

        final CompletableFuture<ManagedLedgerInfo> promise = new CompletableFuture<>();
        store.getManagedLedgerInfo(ledgerName, true, new MetaStoreCallback<ManagedLedgerInfo>() {
            public void operationFailed(MetaStoreException e) {
                promise.completeExceptionally(e);
            }

            public void operationComplete(ManagedLedgerInfo result, Stat version) {
                promise.complete(result);
            }
        });

        ManagedLedgerInfo info = promise.get();
        assertNotNull(info);
        assertEquals(info.getLedgerInfosCount(), 0);
        assertTrue(raceInjected.get());
    }

    @Test(timeOut = 20000)
    void cursorPropertyUpdateGetsANewPublication() throws Exception {
        var ledger = factory.open("publication-test");
        var cursor = ledger.openCursor("c1");
        String path = "/managed-ledgers/publication-test/c1";
        var persisted = new ManagedCursorInfo();
        persisted.parseFrom(metadataStore.get(path).join().orElseThrow().getValue());
        String first = persisted.getPublication();
        assertThat(first).isNotBlank();

        cursor.putCursorProperty("key", "value").join();
        var updated = new ManagedCursorInfo();
        updated.parseFrom(metadataStore.get(path).join().orElseThrow().getValue());
        assertThat(updated.getPublication()).isNotBlank().isNotEqualTo(first);
        ledger.close();
    }

    @Test(timeOut = 20000)
    void callerCanRetryCursorWriteWithTheSamePublication() throws Exception {
        try (var zk = MockZooKeeper.newInstance(); var zkStore = new ZKMetadataStore(zk)) {
            MetaStore store = new MetaStoreImpl(zkStore, executor);
            var create = new ManagedCursorInfo().setCursorsLedgerId(1).setPublication("");
            Stat created = writeCursorInfo(store, create, null);
            String generatedId = create.getPublication();
            assertThat(generatedId).isNotEmpty();
            assertThat(writeCursorInfo(store, create, null).getVersion()).isZero();
            assertThat(create.getPublication()).isEqualTo(generatedId);

            String suppliedId = UUID.randomUUID().toString();
            var update = new ManagedCursorInfo().setCursorsLedgerId(2).setPublication(suppliedId);
            assertThat(writeCursorInfo(store, update, created).getVersion()).isOne();
            assertThat(update.getPublication()).isEqualTo(suppliedId);
            assertThat(writeCursorInfo(store, update, created).getVersion()).isOne();
        }
    }

    @Test(timeOut = 45_000)
    void openCursorRecoversAfterVerificationTimeouts() throws Exception {
        var cluster = new ZooKeeperUtil("/ledgers");
        try {
            cluster.startCluster();
            ZooKeeper zk = cluster.getZooKeeperClient();
            ZooKeeper client = mock(ZooKeeper.class, delegatesTo(zk));
            try (var store = new ZKMetadataStore(client)) {
                var zkFactory = new ManagedLedgerFactoryImpl(store, bkc);
                try {
                    var ledger = zkFactory.open("ledger");
                    String path = "/managed-ledgers/ledger/cursor";
                    var lostCreate = new AtomicBoolean();
                    doAnswer(inv -> {
                        Iterable<Op> ops = inv.getArgument(0);
                        MultiCallback callback = inv.getArgument(1);
                        for (Op op : ops) {
                            if (op.getType() == OpCode.create && op.getPath().equals(path)) {
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
                            callback.processResult(Code.OPERATIONTIMEOUT.intValue(), path, null, null, null);
                        } else {
                            zk.getData(path, false, callback, inv.getArgument(3));
                        }
                        return null;
                    }).when(client).getData(eq(path), eq(false), any(DataCallback.class), any());

                    var cursor = ledger.openCursor("cursor");
                    assertThat(ledger.openCursor("cursor")).isSameAs(cursor);
                    assertThat(lostCreate.get()).isTrue();
                    assertThat(reads.get()).isEqualTo(3);
                    assertThat(zk.exists(path, false).getVersion()).isZero();
                } finally {
                    zkFactory.shutdown();
                }
            }
        } finally {
            cluster.killCluster();
        }
    }

    private static Stat writeCursorInfo(MetaStore store, ManagedCursorInfo info, Stat expected) throws Exception {
        var written = new CompletableFuture<Stat>();
        store.asyncUpdateCursorInfo("ledger", "cursor", info, expected, new MetaStoreCallback<>() {
            @Override
            public void operationComplete(Void ignored, Stat stat) {
                written.complete(stat);
            }

            @Override
            public void operationFailed(MetaStoreException error) {
                written.completeExceptionally(error);
            }
        });
        return written.get(5, TimeUnit.SECONDS);
    }

    @Test(timeOut = 20000)
    void cursorEncodingFailureReachesCallback() throws Exception {
        MetaStoreImpl store = spy(new MetaStoreImpl(metadataStore, executor));
        var info = new ManagedCursorInfo().setCursorsLedgerId(1);
        var failure = new IllegalStateException("Failed to encode cursor");
        doThrow(failure).when(store).compressCursorInfo(info);
        var result = new CompletableFuture<MetaStoreException>();
        store.asyncUpdateCursorInfo("my_test", "c1", info, null, new MetaStoreCallback<>() {
            @Override
            public void operationFailed(MetaStoreException error) {
                result.complete(error);
            }

            @Override
            public void operationComplete(Void ignored, Stat stat) {
                result.completeExceptionally(new AssertionError("Expected cursor encoding to fail"));
            }
        });
        assertThat(result.get().getCause()).isSameAs(failure);
    }

    @Test(timeOut = 20000)
    void updatingCursorNode() throws Exception {
        MetaStore store = new MetaStoreImpl(metadataStore, executor);

        metadataStore.put("/managed-ledgers/my_test", "".getBytes(), Optional.empty()).join();

        final CompletableFuture<Void> promise = new CompletableFuture<>();

        ManagedCursorInfo info = new ManagedCursorInfo().setCursorsLedgerId(1);
        store.asyncUpdateCursorInfo("my_test", "c1", info, null, new MetaStoreCallback<Void>() {
            public void operationFailed(MetaStoreException e) {
                promise.completeExceptionally(e);
            }

            public void operationComplete(Void result, Stat version) {
                // Update again using the version
                metadataStore.failConditional(new MetadataStoreException("error"), (op, path) ->
                        op == FaultInjectionMetadataStore.OperationType.PUT
                                && path.contains("my_test") && path.contains("c1")
                );

                ManagedCursorInfo info = new ManagedCursorInfo().setCursorsLedgerId(2);
                store.asyncUpdateCursorInfo("my_test", "c1", info, version, new MetaStoreCallback<Void>() {
                    public void operationFailed(MetaStoreException e) {
                        // ok
                        promise.complete(null);
                    }

                    @Override
                    public void operationComplete(Void result, Stat version) {
                        promise.completeExceptionally(new Exception("should have failed"));
                    }
                });
            }
        });
        promise.get();
    }

    @Test(timeOut = 20000)
    void updatingMLNode() throws Exception {
        MetaStore store = new MetaStoreImpl(metadataStore, executor);

        metadataStore.put("/managed-ledgers/my_test", "".getBytes(), Optional.empty());

        final CompletableFuture<Void> promise = new CompletableFuture<>();

        store.getManagedLedgerInfo("my_test", false, new MetaStoreCallback<ManagedLedgerInfo>() {
            public void operationFailed(MetaStoreException e) {
                promise.completeExceptionally(e);
            }

            public void operationComplete(ManagedLedgerInfo mlInfo, Stat version) {
                // Update again using the version
                metadataStore.failConditional(new MetadataStoreException.BadVersionException("error"), (op, path) ->
                        op == FaultInjectionMetadataStore.OperationType.PUT
                                && path.contains("my_test")
                );

                store.asyncUpdateLedgerIds("my_test", mlInfo, version, new MetaStoreCallback<Void>() {
                    public void operationFailed(MetaStoreException e) {
                        // ok
                        promise.complete(null);
                    }

                    @Override
                    public void operationComplete(Void result, Stat version) {
                        promise.completeExceptionally(new Exception("should have failed"));
                    }
                });
            }
        });

        promise.get();
    }

    @Test(timeOut = 20000)
    void unwatchKeepsWatchOfAnotherCallback() throws Exception {
        MetaStore store = new MetaStoreImpl(metadataStore, executor);
        metadataStore.put("/managed-ledgers/my_test", "".getBytes(), Optional.empty()).join();
        CompletableFuture<ManagedLedgerInfo> notified = new CompletableFuture<>();
        MetaStore.UpdateCallback<ManagedLedgerInfo> replacedCallback = (mlInfo, stat) -> { };
        store.watchManagedLedgerInfo("my_test", replacedCallback);
        store.watchManagedLedgerInfo("my_test", (mlInfo, stat) -> notified.complete(mlInfo));

        // Unwatching with the replaced callback, as an instance closed after another one started watching would
        store.unwatchManagedLedgerInfo("my_test", replacedCallback);
        metadataStore.put("/managed-ledgers/my_test", "".getBytes(), Optional.empty()).join();

        assertNotNull(notified.get(10, TimeUnit.SECONDS));
    }

    @Test
    public void testGetChildrenWatch() throws Exception {
        MetadataCache<MyClass> objCache1 = metadataStore.getMetadataCache(MyClass.class);

        String path = "/managed-ledgers/prop-xyz/ns1/persistent";
        assertTrue(objCache1.getChildren(path).get().isEmpty());

        metadataStore.put("/managed-ledgers/prop-xyz/ns1/persistent/t1", "".getBytes(), Optional.empty()).join();

        ManagedLedgerTest.retryStrategically((test) -> {
            try {
                return !objCache1.getChildren(path).get().isEmpty();
            } catch (Exception e) {
                // Ok
            }
            return false;
        }, 5, 1000);
        assertFalse(objCache1.getChildren(path).get().isEmpty());
    }
}
