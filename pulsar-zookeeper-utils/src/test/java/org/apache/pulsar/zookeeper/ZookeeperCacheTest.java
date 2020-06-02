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
package org.apache.pulsar.zookeeper;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;

import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.Collections;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperCacheTest {
    private static final Logger log = LoggerFactory.getLogger(ZookeeperCacheTest.class);
    private MockZooKeeper zkClient;
    private OrderedScheduler executor;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeMethod
    void setup() throws Exception {
        zkClient = MockZooKeeper.newInstance(MoreExecutors.newDirectExecutorService());
    }

    @AfterMethod
    void teardown() throws Exception {
        zkClient.shutdown();
    }

    @BeforeClass
    void classSetup() throws Exception {
        executor = OrderedScheduler.newSchedulerBuilder().numThreads(1).name("ZookeeperCacheTest").build();
        scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterClass
    void classTeardown() throws Exception {
        executor.shutdown();
        scheduledExecutor.shutdown();
    }


    @Test(timeOut = 10000)
    public void testSimpleCache() throws Exception {

        ZooKeeperCache zkCacheService = new LocalZooKeeperCache(zkClient, 30, executor);
        ZooKeeperDataCache<String> zkCache = new ZooKeeperDataCache<String>(zkCacheService) {
            @Override
            public String deserialize(String key, byte[] content) throws Exception {
                return new String(content);
            }
        };

        String value = "test";
        zkClient.create("/my_test", value.getBytes(), null, null);

        assertEquals(zkCache.get("/my_test").get(), value);
        assertEquals(zkCache.getDataIfPresent("/my_test"), value);

        String newValue = "test2";

        zkClient.setData("/my_test", newValue.getBytes(), -1);

        // Wait for the watch to be triggered
        Thread.sleep(100);

        assertEquals(zkCache.get("/my_test").get(), newValue);

        zkCacheService.process(new WatchedEvent(Event.EventType.None, KeeperState.Expired, null));
        assertEquals(zkCache.get("/my_test").get(), newValue);

        zkClient.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
                return op == MockZooKeeper.Op.GET
                    && path.equals("/other");
            });

        assertEquals(zkCache.get("/my_test").get(), newValue);
        try {
            zkCache.get("/other");
            fail("shuld have thrown exception");
        } catch (Exception e) {
            // Ok
        }
    }

    @Test(timeOut = 10000)
    public void testChildrenCache() throws Exception {
        zkClient.create("/test", new byte[0], null, null);

        ZooKeeperCache zkCacheService = new LocalZooKeeperCache(zkClient, 30, executor);
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
        zkClient.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
                return op == MockZooKeeper.Op.GET_CHILDREN
                    && path.equals("/test");
            });

        try {
            cache.get();
            fail("shuld have thrown exception");
        } catch (Exception e) {
            // Ok
        }

        assertEquals(notificationCount.get(), 3);
    }

    @Test(timeOut = 10000)
    public void testChildrenCacheZnodeCreatedAfterCache() throws Exception {

        ZooKeeperCache zkCacheService = new LocalZooKeeperCache(zkClient, 30, executor);
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
        assertEquals(cache.get(), Collections.emptySet());

        zkClient.create("/test", new byte[0], null, null);
        zkClient.create("/test/z1", new byte[0], null, null);

        // Wait for cache to be updated in background
        while (notificationCount.get() < 1) {
            Thread.sleep(1);
        }

        final int recvNotifications = notificationCount.get();

        assertEquals(cache.get(), new TreeSet<String>(Lists.newArrayList("z1")));
        assertEquals(cache.get("/test"), new TreeSet<String>(Lists.newArrayList("z1")));
        assertTrue(recvNotifications == 1 || recvNotifications == 2);

        zkClient.delete("/test/z1", -1);
        while (notificationCount.get() < (recvNotifications + 1)) {
            Thread.sleep(1);
        }

        assertTrue(cache.get().isEmpty());
        assertTrue(cache.get().isEmpty());
        zkCacheService.process(new WatchedEvent(Event.EventType.None, KeeperState.Expired, null));
        zkClient.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
                return op == MockZooKeeper.Op.GET_CHILDREN
                    && path.equals("/test");
            });

        try {
            cache.get();
            fail("should have thrown exception");
        } catch (Exception e) {
            // Ok
        }
    }

    @Test(timeOut = 10000)
    public void testExistsCache() throws Exception {
        // Check existence after creation of the node
        zkClient.create("/test", new byte[0], null, null);
        Thread.sleep(20);
        ZooKeeperCache zkCacheService = new LocalZooKeeperCache(zkClient, 30, executor);
        boolean exists = zkCacheService.exists("/test");
        Assert.assertTrue(exists, "/test should exists in the cache");

        // Check existence after deletion if the node
        zkClient.delete("/test", -1);
        Thread.sleep(20);
        boolean shouldNotExist = zkCacheService.exists("/test");
        Assert.assertFalse(shouldNotExist, "/test should not exist in the cache");
    }

    @Test(timeOut = 10000)
    public void testInvalidateCache() throws Exception {
        zkClient.create("/test", new byte[0], null, null);
        zkClient.create("/test/c1", new byte[0], null, null);
        zkClient.create("/test/c2", new byte[0], null, null);
        Thread.sleep(20);
        ZooKeeperCache zkCacheService = new LocalZooKeeperCache(zkClient, 30, executor);
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

    @Test(timeOut = 10000)
    public void testGlobalZooKeeperCache() throws Exception {
        MockZooKeeper zkc = MockZooKeeper.newInstance();
        ZooKeeperClientFactory zkClientfactory = new ZooKeeperClientFactory() {
            @Override
            public CompletableFuture<ZooKeeper> create(String serverList, SessionType sessionType,
                    int zkSessionTimeoutMillis) {
                return CompletableFuture.completedFuture(zkc);
            }
        };

        GlobalZooKeeperCache zkCacheService = new GlobalZooKeeperCache(zkClientfactory, -1, 30, "", executor,
                scheduledExecutor, 300);
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
        zkClient.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
                return op == MockZooKeeper.Op.GET
                    && path.equals("/other");
            });

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

        // Update shouldn't happen after the last check
        assertEquals(notificationCount.get(), 1);
    }

    /**
     * Verifies that blocking call on zkCache-callback will not introduce deadlock because zkCache completes
     * future-result with different thread than zookeeper-client thread.
     *
     * @throws Exception
     */
    @Test(timeOut = 2000)
    public void testZkCallbackThreadStuck() throws Exception {
        OrderedScheduler executor = OrderedScheduler.newSchedulerBuilder().build();
        ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(2);
        ExecutorService zkExecutor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("mockZk"));
        // add readOpDelayMs so, main thread will not serve zkCacahe-returned future and let zkExecutor-thread handle
        // callback-result process
        MockZooKeeper zkClient = MockZooKeeper.newInstance(zkExecutor, 100);
        ZooKeeperCache zkCacheService = new LocalZooKeeperCache(zkClient, 30, executor);
        ZooKeeperDataCache<String> zkCache = new ZooKeeperDataCache<String>(zkCacheService) {
            @Override
            public String deserialize(String key, byte[] content) throws Exception {
                return new String(content);
            }
        };

        String value = "test";
        String key = "/" + UUID.randomUUID().toString().substring(0, 8);
        String key1 = "/" + UUID.randomUUID().toString().substring(0, 8);
        String key2 = "/" + UUID.randomUUID().toString().substring(0, 8);
        zkClient.create(key, value.getBytes(), null, null);
        zkClient.create(key1, value.getBytes(), null, null);
        zkClient.create(key2, value.getBytes(), null, null);

        CountDownLatch latch = new CountDownLatch(1);

        zkCache.getAsync(key).thenAccept(val -> {
            try {
                zkCache.get(key1);
            } catch (Exception e) {
                fail("failed to get " + key2, e);
            }
            latch.countDown();
        });

        latch.await();
        executor.shutdown();
        zkExecutor.shutdown();
        scheduledExecutor.shutdown();
    }

    /**
     * <pre>
     * Verifies that if {@link ZooKeeperCache} fails to fetch data into the cache then
     * (1) it invalidates failed future so, next time it helps to get fresh data from zk
     * (2) handles zk.getData() unexpected exception if zkSession is lost
     * </pre>
     *
     * @throws Exception
     */
    @Test(timeOut = 10000)
    public void testInvalidateCacheOnFailure() throws Exception {
        ExecutorService zkExecutor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("mockZk"));
        // add readOpDelayMs so, main thread will not serve zkCacahe-returned future and let zkExecutor-thread handle
        // callback-result process
        MockZooKeeper zkClient = MockZooKeeper.newInstance(zkExecutor, 100);
        ZooKeeperCache zkCacheService = new LocalZooKeeperCache(zkClient, 30, executor);

        final AtomicInteger count = new AtomicInteger(0);
        ZooKeeperDataCache<String> zkCache = new ZooKeeperDataCache<String>(zkCacheService) {
            @Override
            public String deserialize(String key, byte[] content) throws Exception {
                if (count.getAndIncrement() == 0) {
                    throw new NullPointerException("data is null");
                } else {
                    return new String(content);
                }
            }
        };

        String value = "test";
        String key1 = "/zkDesrializationExceptionTest";
        String key2 = "/zkSessionExceptionTest";
        zkClient.create(key1, value.getBytes(), null, null);
        zkClient.create(key2, value.getBytes(), null, null);

        // (1) deserialization will fail so, result should be exception
        try {
            zkCache.getAsync(key1).get();
            fail("it should have failed with NPE");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof NullPointerException);
        }

        // (2) sleep to let cache to be invalidated async
        Thread.sleep(1000);
        // (3) now, cache should be invalidate failed-future and should refetch the data
        assertEquals(zkCache.getAsync(key1).get().get(), value);

        // (4) make zk-session invalid
        ZooKeeper zkSession = zkCacheService.zkSession.get();
        zkCacheService.zkSession.set(null);

        try {
            zkCache.getAsync(key2).get();
            fail("it should have failed with NPE");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof NullPointerException);
        }

        // global-Zk session is connected now
        zkCacheService.zkSession.set(zkSession);
        // (5) sleep to let cache to be invalidated async
        Thread.sleep(1000);
        // (6) now, cache should be invalidate failed-future and should refetch the data
        assertEquals(zkCache.getAsync(key1).get().get(), value);
        zkExecutor.shutdown();
    }

    /**
     * This tests verifies that {{@link ZooKeeperDataCache} invalidates the cache if the get-operation time-out on that
     * path.
     *
     * @throws Exception
     */
    @Test
    public void testTimedOutZKCacheRequestInvalidates() throws Exception {

        OrderedScheduler executor = OrderedScheduler.newSchedulerBuilder().build();
        ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(2);
        ExecutorService zkExecutor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("mockZk"));
        MockZooKeeper zkSession = spy(MockZooKeeper.newInstance(MoreExecutors.newDirectExecutorService()));

        String path = "test";
        doNothing().when(zkSession).getData(anyString(), any(Watcher.class), any(DataCallback.class), any());
        zkClient.create("/test", new byte[0], null, null);

        // add readOpDelayMs so, main thread will not serve zkCacahe-returned future and let zkExecutor-thread handle
        // callback-result process
        ZooKeeperCache zkCacheService = new LocalZooKeeperCache(zkSession, 1, executor);
        ZooKeeperDataCache<String> zkCache = new ZooKeeperDataCache<String>(zkCacheService) {
            @Override
            public String deserialize(String key, byte[] content) throws Exception {
                return new String(content);
            }
        };

        // try to do get on the path which will time-out and async-cache will have non-completed Future
        try {
            zkCache.get(path);
        } catch (Exception e) {
            // Ok
        }

        retryStrategically((test) -> {
            return zkCacheService.dataCache.getIfPresent(path) == null;
        }, 5, 1000);

        assertNull(zkCacheService.dataCache.getIfPresent(path));

        executor.shutdown();
        zkExecutor.shutdown();
        scheduledExecutor.shutdown();
    }

    private static void retryStrategically(Predicate<Void> predicate, int retryCount, long intSleepTimeInMillis)
            throws Exception {
        for (int i = 0; i < retryCount; i++) {
            if (predicate.test(null) || i == (retryCount - 1)) {
                break;
            }
            Thread.sleep(intSleepTimeInMillis + (intSleepTimeInMillis * i));
        }
    }
}
