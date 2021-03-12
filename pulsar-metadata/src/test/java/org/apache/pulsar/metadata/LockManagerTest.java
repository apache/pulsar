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
package org.apache.pulsar.metadata;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;

import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException.LockBusyException;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.coordination.LockManager;
import org.apache.pulsar.metadata.api.coordination.ResourceLock;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.coordination.impl.CoordinationServiceImpl;
import org.testng.annotations.Test;

public class LockManagerTest extends BaseMetadataStoreTest {

    @Test(dataProvider = "impl")
    public void acquireLocks(String provider, String url) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(url, MetadataStoreConfig.builder().build());

        @Cleanup
        CoordinationService coordinationService = new CoordinationServiceImpl(store);

        @Cleanup
        LockManager<String> lockManager = coordinationService.getLockManager(String.class);

        assertEquals(lockManager.listLocks("/my/path").join(), Collections.emptyList());
        assertEquals(lockManager.readLock("/my/path/1").join(), Optional.empty());

        ResourceLock<String> lock1 = lockManager.acquireLock("/my/path/1", "lock-1").join();
        assertEquals(lockManager.listLocks("/my/path").join(), Collections.singletonList("1"));
        assertEquals(lockManager.readLock("/my/path/1").join(), Optional.of("lock-1"));
        assertEquals(lock1.getPath(), "/my/path/1");
        assertEquals(lock1.getValue(), "lock-1");

        CountDownLatch latchLock1 = new CountDownLatch(1);
        lock1.getLockExpiredFuture().thenRun(() -> {
            latchLock1.countDown();
        });
        assertEquals(latchLock1.getCount(), 1);

        try {
            lockManager.acquireLock("/my/path/1", "lock-2").join();
            fail("should have failed");
        } catch (CompletionException e) {
            assertException(e, LockBusyException.class);
        }

        assertEquals(lockManager.listLocks("/my/path").join(), Collections.singletonList("1"));
        assertEquals(lockManager.readLock("/my/path/1").join(), Optional.of("lock-1"));

        assertEquals(latchLock1.getCount(), 1);

        lock1.release().join();
        assertEquals(lockManager.listLocks("/my/path").join(), Collections.emptyList());
        assertEquals(lockManager.readLock("/my/path/1").join(), Optional.empty());

        // The future should have been triggered before the release is complete
        latchLock1.await(0, TimeUnit.SECONDS);

        // Double release shoud be a no-op
        lock1.release().join();

        ResourceLock<String> lock2 = lockManager.acquireLock("/my/path/1", "lock-1").join();
        assertEquals(lockManager.listLocks("/my/path").join(), Collections.singletonList("1"));
        assertEquals(lockManager.readLock("/my/path/1").join(), Optional.of("lock-1"));
        assertEquals(lock2.getPath(), "/my/path/1");
        assertEquals(lock2.getValue(), "lock-1");
    }

    @Test(dataProvider = "impl")
    public void cleanupOnClose(String provider, String url) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(url, MetadataStoreConfig.builder().build());

        @Cleanup
        CoordinationService coordinationService = new CoordinationServiceImpl(store);

        LockManager<String> lockManager = coordinationService.getLockManager(String.class);

        assertEquals(lockManager.listLocks("/my/path").join(), Collections.emptyList());
        assertEquals(lockManager.readLock("/my/path/1").join(), Optional.empty());

        lockManager.acquireLock("/my/path/1", "lock-1").join();
        assertEquals(lockManager.listLocks("/my/path").join(), Collections.singletonList("1"));
        assertEquals(lockManager.readLock("/my/path/1").join(), Optional.of("lock-1"));

        lockManager.acquireLock("/my/path/2", "lock-2").join();
        assertEquals(lockManager.listLocks("/my/path").join(), new ArrayList<>(Arrays.asList("1", "2")));
        assertEquals(lockManager.readLock("/my/path/2").join(), Optional.of("lock-2"));

        lockManager.close();

        lockManager = coordinationService.getLockManager(String.class);
        assertEquals(lockManager.listLocks("/my/path").join(), Collections.emptyList());
        assertEquals(lockManager.readLock("/my/path/1").join(), Optional.empty());
        assertEquals(lockManager.readLock("/my/path/2").join(), Optional.empty());
    }

    @Test(dataProvider = "impl")
    public void updateValue(String provider, String url) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(url, MetadataStoreConfig.builder().build());

        MetadataCache<String> cache = store.getMetadataCache(String.class);

        @Cleanup
        CoordinationService coordinationService = new CoordinationServiceImpl(store);

        @Cleanup
        LockManager<String> lockManager = coordinationService.getLockManager(String.class);

        ResourceLock<String> lock = lockManager.acquireLock("/my/path/1", "lock-1").join();
        assertEquals(lock.getValue(), "lock-1");
        assertEquals(cache.get("/my/path/1").join().get(), "lock-1");

        lock.updateValue("value-2").join();
        assertEquals(lock.getValue(), "value-2");
        assertEquals(cache.get("/my/path/1").join().get(), "value-2");
    }
}
