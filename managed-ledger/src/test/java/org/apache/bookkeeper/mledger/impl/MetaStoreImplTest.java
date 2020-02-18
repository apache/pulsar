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

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.impl.zookeeper.ZKMetadataStore;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs;
import org.testng.annotations.Test;

public class MetaStoreImplTest extends MockedBookKeeperTestCase {

    @Test
    void getMLList() throws Exception {
        MetaStore store = new MetaStoreImpl(new ZKMetadataStore(zkc), executor);

        zkc.failNow(Code.CONNECTIONLOSS);

        try {
            store.getManagedLedgers();
            fail("should fail in getting the list");
        } catch (MetaStoreException e) {
            // ok
        }
    }

    @Test
    void deleteNonExistingML() throws Exception {
        MetaStore store = new MetaStoreImpl(new ZKMetadataStore(zkc), executor);

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
        MetaStore store = new MetaStoreImpl(new ZKMetadataStore(zkc), executor);

        zkc.create("/managed-ledgers/my_test", "non-valid".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

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
        MetaStore store = new MetaStoreImpl(new ZKMetadataStore(zkc), executor);

        zkc.create("/managed-ledgers/my_test", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zkc.create("/managed-ledgers/my_test/c1", "non-valid".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

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
        MetaStore store = new MetaStoreImpl(new ZKMetadataStore(zkc), executor);

        final CountDownLatch latch = new CountDownLatch(1);

        zkc.failAfter(1, Code.CONNECTIONLOSS);

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
    void updatingCursorNode() throws Exception {
        MetaStore store = new MetaStoreImpl(new ZKMetadataStore(zkc), executor);

        zkc.create("/managed-ledgers/my_test", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        final CountDownLatch latch = new CountDownLatch(1);

        ManagedCursorInfo info = ManagedCursorInfo.newBuilder().setCursorsLedgerId(1).build();
        store.asyncUpdateCursorInfo("my_test", "c1", info, null, new MetaStoreCallback<Void>() {
            public void operationFailed(MetaStoreException e) {
                fail("should have succeeded");
            }

            public void operationComplete(Void result, Stat version) {
                // Update again using the version
                zkc.failNow(Code.CONNECTIONLOSS);

                ManagedCursorInfo info = ManagedCursorInfo.newBuilder().setCursorsLedgerId(2).build();
                store.asyncUpdateCursorInfo("my_test", "c1", info, version, new MetaStoreCallback<Void>() {
                    public void operationFailed(MetaStoreException e) {
                        // ok
                        latch.countDown();
                    }

                    @Override
                    public void operationComplete(Void result, Stat version) {
                        fail("should have failed");
                    }
                });
            }
        });

        latch.await();
    }

    @Test(timeOut = 20000)
    void updatingMLNode() throws Exception {
        MetaStore store = new MetaStoreImpl(new ZKMetadataStore(zkc), executor);

        zkc.create("/managed-ledgers/my_test", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        final CountDownLatch latch = new CountDownLatch(1);

        store.getManagedLedgerInfo("my_test", false, new MetaStoreCallback<ManagedLedgerInfo>() {
            public void operationFailed(MetaStoreException e) {
                fail("should have succeeded");
            }

            public void operationComplete(ManagedLedgerInfo mlInfo, Stat version) {
                // Update again using the version
                zkc.failNow(Code.BADVERSION);

                store.asyncUpdateLedgerIds("my_test", mlInfo, version, new MetaStoreCallback<Void>() {
                    public void operationFailed(MetaStoreException e) {
                        // ok
                        latch.countDown();
                    }

                    @Override
                    public void operationComplete(Void result, Stat version) {
                        fail("should have failed");
                    }
                });
            }
        });

        latch.await();
    }
}
