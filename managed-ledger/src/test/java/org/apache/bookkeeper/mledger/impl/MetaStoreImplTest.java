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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Stat;
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

        store.getManagedLedgerInfo("my_test", false, new MetaStoreCallback<MLDataFormats.ManagedLedgerInfo>() {
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
        store.asyncGetCursorInfo("my_test", "c1", new MetaStoreCallback<MLDataFormats.ManagedCursorInfo>() {

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

        store.getManagedLedgerInfo("my_test", false, new MetaStoreCallback<MLDataFormats.ManagedLedgerInfo>() {
            public void operationFailed(MetaStoreException e) {
                promise.complete(null);
            }

            public void operationComplete(ManagedLedgerInfo result, Stat version) {
                promise.completeExceptionally(new Exception("Operation should have failed"));
            }
        });
        promise.get();
    }

    @Test(timeOut = 20000)
    void updatingCursorNode() throws Exception {
        MetaStore store = new MetaStoreImpl(metadataStore, executor);

        metadataStore.put("/managed-ledgers/my_test", "".getBytes(), Optional.empty()).join();

        final CompletableFuture<Void> promise = new CompletableFuture<>();

        ManagedCursorInfo info = ManagedCursorInfo.newBuilder().setCursorsLedgerId(1).build();
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

                ManagedCursorInfo info = ManagedCursorInfo.newBuilder().setCursorsLedgerId(2).build();
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
