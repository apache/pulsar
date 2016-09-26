/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.zookeeper;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;

@Test
public class ZookeeperCacheTest {
    private MockZooKeeper zkClient;

    @BeforeMethod
    void setup() throws Exception {
        zkClient = MockZooKeeper.newInstance(MoreExecutors.sameThreadExecutor());
    }

    @AfterMethod
    void teardown() throws Exception {
        zkClient.shutdown();
    }

    @Test
    void testSimpleCache() throws Exception {
        ZooKeeperCache zkCacheService = new LocalZooKeeperCache(zkClient, null /* no executors in unit test */);
        ZooKeeperDataCache<String> zkCache = new ZooKeeperDataCache<String>(zkCacheService) {
            @Override
            public String deserialize(String key, byte[] content) throws Exception {
                return new String(content);
            }
        };

        String value = "test";
        zkClient.create("/my_test", value.getBytes(), null, null);

        assertEquals(zkCache.get("/my_test").get(), value);

        String newValue = "test2";

        zkClient.setData("/my_test", newValue.getBytes(), -1);

        // Wait for the watch to be triggered
        Thread.sleep(100);

        assertEquals(zkCache.get("/my_test").get(), newValue);

        zkCacheService.process(new WatchedEvent(Event.EventType.None, KeeperState.Expired, null));
        assertEquals(zkCache.get("/my_test").get(), newValue);

        zkClient.failNow(Code.SESSIONEXPIRED);

        assertEquals(zkCache.get("/my_test").get(), newValue);
        try {
            zkCache.get("/other");
            fail("shuld have thrown exception");
        } catch (Exception e) {
            // Ok
        }
    }

    @Test
    void testChildrenCache() throws Exception {
        OrderedSafeExecutor executor = new OrderedSafeExecutor(1, "test");
        zkClient.create("/test", new byte[0], null, null);

        ZooKeeperCache zkCacheService = new LocalZooKeeperCache(zkClient, executor);
        ZooKeeperChildrenCache cache = new ZooKeeperChildrenCache(zkCacheService, "/test");

        // Create callback counter
        AtomicInteger notificationCount = new AtomicInteger(0);
        ZooKeeperCacheListener<Set<String>> counter = (path, data, stat) -> {
            notificationCount.incrementAndGet();
        };

        // Register counter twice and unregister once, so callback should be counted correctly
        cache.registerListener(counter);
        cache.registerListener(counter);
        cache.unregisterListener(counter);
        assertEquals(notificationCount.get(), 0);
        assertEquals(cache.get(), Sets.newTreeSet());

        zkClient.create("/test/z1", new byte[0], null, null);
        zkClient.create("/test/z2", new byte[0], null, null);

        // Wait for cache to be updated in background
        while (notificationCount.get() < 2) {
            Thread.sleep(1);
        }

        assertEquals(cache.get(), new TreeSet<String>(Lists.newArrayList("z1", "z2")));
        assertEquals(cache.get("/test"), new TreeSet<String>(Lists.newArrayList("z1", "z2")));
        assertEquals(notificationCount.get(), 2);

        zkClient.delete("/test/z2", -1);
        while (notificationCount.get() < 3) {
            Thread.sleep(1);
        }

        assertEquals(cache.get(), new TreeSet<String>(Lists.newArrayList("z1")));
        assertEquals(cache.get(), new TreeSet<String>(Lists.newArrayList("z1")));
        zkCacheService.process(new WatchedEvent(Event.EventType.None, KeeperState.Expired, null));
        zkClient.failNow(Code.SESSIONEXPIRED);

        try {
            cache.get();
            fail("shuld have thrown exception");
        } catch (Exception e) {
            // Ok
        }

        assertEquals(notificationCount.get(), 3);
    }

    @Test
    void testExistsCache() throws Exception {
        // Check existence after creation of the node
        zkClient.create("/test", new byte[0], null, null);
        Thread.sleep(20);
        ZooKeeperCache zkCacheService = new LocalZooKeeperCache(zkClient, null /* no executor in unit test */);
        boolean exists = zkCacheService.exists("/test");
        Assert.assertTrue(exists, "/test should exists in the cache");

        // Check existence after deletion if the node
        zkClient.delete("/test", -1);
        Thread.sleep(20);
        boolean shouldNotExist = zkCacheService.exists("/test");
        Assert.assertFalse(shouldNotExist, "/test should not exist in the cache");
    }

    @Test
    void testInvalidateCache() throws Exception {
        zkClient.create("/test", new byte[0], null, null);
        zkClient.create("/test/c1", new byte[0], null, null);
        zkClient.create("/test/c2", new byte[0], null, null);
        Thread.sleep(20);
        ZooKeeperCache zkCacheService = new LocalZooKeeperCache(zkClient, null /* no executor in unit test */);
        boolean exists = zkCacheService.exists("/test");
        Assert.assertTrue(exists, "/test should exists in the cache");

        assertNull(zkCacheService.getChildrenIfPresent("/test"));
        assertNotNull(zkCacheService.getChildren("/test"));
        assertNotNull(zkCacheService.getChildrenIfPresent("/test"));
        zkCacheService.invalidateAllChildren();
        assertNull(zkCacheService.getChildrenIfPresent("/test"));

        assertNull(zkCacheService.getDataIfPresent("/test"));
        assertNotNull(zkCacheService.getData("/test", Deserializers.STRING_DESERIALIZER));
        assertNotNull(zkCacheService.getDataIfPresent("/test"));
        zkCacheService.invalidateData("/test");
        assertNull(zkCacheService.getDataIfPresent("/test"));

        assertNotNull(zkCacheService.getChildren("/test"));
        assertNotNull(zkCacheService.getData("/test", Deserializers.STRING_DESERIALIZER));
        zkCacheService.invalidateAll();
        assertNull(zkCacheService.getChildrenIfPresent("/test"));
        assertNull(zkCacheService.getDataIfPresent("/test"));

        assertNotNull(zkCacheService.getChildren("/test"));
        zkCacheService.invalidateRoot("/test");
        assertNull(zkCacheService.getChildrenIfPresent("/test"));
    }

    @Test
    void testGlobalZooKeeperCache() throws Exception {
        OrderedSafeExecutor executor = new OrderedSafeExecutor(1, "test");
        ScheduledExecutorService scheduledExecutor = new ScheduledThreadPoolExecutor(1);
        MockZooKeeper zkc = MockZooKeeper.newInstance();
        ZooKeeperClientFactory zkClientfactory = new ZooKeeperClientFactory() {
            @Override
            public CompletableFuture<ZooKeeper> create(String serverList, SessionType sessionType,
                    int zkSessionTimeoutMillis) {
                return CompletableFuture.completedFuture(zkc);
            }
        };

        GlobalZooKeeperCache zkCacheService = new GlobalZooKeeperCache(zkClientfactory, -1, "", executor,
                scheduledExecutor);
        zkCacheService.start();
        zkClient = (MockZooKeeper) zkCacheService.getZooKeeper();
        ZooKeeperDataCache<String> zkCache = new ZooKeeperDataCache<String>(zkCacheService) {
            @Override
            public String deserialize(String key, byte[] content) throws Exception {
                return new String(content);
            }
        };

        // Create callback counter
        AtomicInteger notificationCount = new AtomicInteger(0);
        ZooKeeperCacheListener<String> counter = (path, data, stat) -> {
            notificationCount.incrementAndGet();
        };

        // Register counter twice and unregister once, so callback should be counted correctly
        zkCache.registerListener(counter);
        zkCache.registerListener(counter);
        zkCache.unregisterListener(counter);

        String value = "test";
        zkClient.create("/my_test", value.getBytes(), null, null);

        assertEquals(zkCache.get("/my_test").get(), value);

        String newValue = "test2";

        // case 1: update and create znode directly and verify that the cache is retrieving the correct data
        assertEquals(notificationCount.get(), 0);
        zkClient.setData("/my_test", newValue.getBytes(), -1);
        zkClient.create("/my_test2", value.getBytes(), null, null);

        // Wait for the watch to be triggered
        while (notificationCount.get() < 1) {
            Thread.sleep(1);
        }

        // retrieve the data from the cache and verify it is the updated/new data
        assertEquals(zkCache.get("/my_test").get(), newValue);
        assertEquals(zkCache.get("/my_test2").get(), value);

        // The callback method should be called just only once
        assertEquals(notificationCount.get(), 1);

        // case 2: force the ZooKeeper session to be expired and verify that the data is still accessible
        zkCacheService.process(new WatchedEvent(Event.EventType.None, KeeperState.Expired, null));
        assertEquals(zkCache.get("/my_test").get(), newValue);
        assertEquals(zkCache.get("/my_test2").get(), value);

        // case 3: update the znode directly while the client session is marked as expired. Verify that the new updates
        // is not seen in the cache
        zkClient.create("/other", newValue.getBytes(), null, null);
        zkClient.failNow(Code.SESSIONEXPIRED);
        assertEquals(zkCache.get("/my_test").get(), newValue);
        assertEquals(zkCache.get("/my_test2").get(), value);
        try {
            zkCache.get("/other");
            fail("shuld have thrown exception");
        } catch (Exception e) {
            // Ok
        }

        // case 4: directly delete the znode while the session is not re-connected yet. Verify that the deletion is not
        // seen by the cache
        zkClient.failAfter(-1, Code.OK);
        zkClient.delete("/my_test2", -1);
        zkCacheService.process(new WatchedEvent(Event.EventType.None, KeeperState.SyncConnected, null));
        assertEquals(zkCache.get("/other").get(), newValue);

        // Make sure that the value is now directly from ZK and deleted
        assertFalse(zkCache.get("/my_test2").isPresent());

        // case 5: trigger a ZooKeeper disconnected event and make sure the cache content is not changed.
        zkCacheService.process(new WatchedEvent(Event.EventType.None, KeeperState.Disconnected, null));
        zkClient.create("/other2", newValue.getBytes(), null, null);

        // case 6: trigger a ZooKeeper SyncConnected event and make sure that the cache content is invalidated s.t. we
        // can see the updated content now
        zkCacheService.process(new WatchedEvent(Event.EventType.None, KeeperState.SyncConnected, null));
        // make sure that we get it
        assertEquals(zkCache.get("/other2").get(), newValue);

        zkCacheService.close();
        executor.shutdown();

        // Update shouldn't happen after the last check
        assertEquals(notificationCount.get(), 1);
    }
}
