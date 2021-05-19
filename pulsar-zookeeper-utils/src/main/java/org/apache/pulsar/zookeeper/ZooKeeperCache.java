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

import static com.google.common.base.Preconditions.checkNotNull;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.stats.CacheMetricsCollector;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per ZK client ZooKeeper cache supporting ZNode data and children list caches. A cache entry is identified, accessed
 * and invalidated by the ZNode path. For the data cache, ZNode data parsing is done at request time with the given
 * {@link Deserializer} argument.
 *
 */
public abstract class ZooKeeperCache implements Watcher {

    /**
     *
     * @param <T> the type of zookeeper content
     */
    public static interface Deserializer<T> {
        T deserialize(String key, byte[] content) throws Exception;
    }

    public static interface CacheUpdater<T> {
        public void registerListener(ZooKeeperCacheListener<T> listner);

        public void unregisterListener(ZooKeeperCacheListener<T> listner);

        public void reloadCache(String path);
    }

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperCache.class);

    public static final String ZK_CACHE_INSTANCE = "zk_cache_instance";

    protected final AsyncLoadingCache<String, Pair<Entry<Object, Stat>, Long>> dataCache;
    protected final AsyncLoadingCache<String, Set<String>> childrenCache;
    protected final AsyncLoadingCache<String, Boolean> existsCache;
    private final OrderedExecutor executor;
    private final OrderedExecutor backgroundExecutor = OrderedExecutor.newBuilder().name("zk-cache-background").numThreads(2).build();
    private boolean shouldShutdownExecutor;
    private final int zkOperationTimeoutSeconds;
    private static final int DEFAULT_CACHE_EXPIRY_SECONDS = 300; //5 minutes
    private final int cacheExpirySeconds;

    protected AtomicReference<ZooKeeper> zkSession = new AtomicReference<ZooKeeper>(null);

    public ZooKeeperCache(String cacheName, ZooKeeper zkSession, int zkOperationTimeoutSeconds, OrderedExecutor executor) {
        this(cacheName, zkSession, zkOperationTimeoutSeconds, executor, DEFAULT_CACHE_EXPIRY_SECONDS);
    }
    
    public ZooKeeperCache(String cacheName, ZooKeeper zkSession, int zkOperationTimeoutSeconds,
            OrderedExecutor executor, int cacheExpirySeconds) {
        checkNotNull(executor);
        this.zkOperationTimeoutSeconds = zkOperationTimeoutSeconds;
        this.executor = executor;
        this.zkSession.set(zkSession);
        this.shouldShutdownExecutor = false;
        this.cacheExpirySeconds = cacheExpirySeconds;

        this.dataCache = Caffeine.newBuilder()
                .recordStats()
                .buildAsync((key, executor1) -> null);

        this.childrenCache = Caffeine.newBuilder()
                .recordStats()
                .expireAfterWrite(cacheExpirySeconds, TimeUnit.SECONDS)
                .buildAsync((key, executor1) -> null);

        this.existsCache = Caffeine.newBuilder()
                .recordStats()
                .expireAfterWrite(cacheExpirySeconds, TimeUnit.SECONDS)
                .buildAsync((key, executor1) -> null);

        CacheMetricsCollector.CAFFEINE.addCache(cacheName + "-data", dataCache);
        CacheMetricsCollector.CAFFEINE.addCache(cacheName + "-children", childrenCache);
        CacheMetricsCollector.CAFFEINE.addCache(cacheName + "-exists", existsCache);
    }

    public ZooKeeperCache(String cacheName, ZooKeeper zkSession, int zkOperationTimeoutSeconds) {
        this(cacheName, zkSession, zkOperationTimeoutSeconds,
                OrderedExecutor.newBuilder().name("zk-cache-callback-executor").build());
        this.shouldShutdownExecutor = true;
    }

    public ZooKeeper getZooKeeper() {
        return this.zkSession.get();
    }

    public <T> void process(WatchedEvent event, final CacheUpdater<T> updater) {
        final String path = event.getPath();
        if (path != null) {
            dataCache.synchronous().invalidate(path);
            childrenCache.synchronous().invalidate(path);
            // sometimes zk triggers one watch per zk-session and if zkDataCache and ZkChildrenCache points to this
            // ZookeeperCache instance then ZkChildrenCache may not invalidate for it's parent. Therefore, invalidate
            // cache for parent if child is created/deleted
            if (event.getType().equals(EventType.NodeCreated) || event.getType().equals(EventType.NodeDeleted)) {
                childrenCache.synchronous().invalidate(ZkUtils.getParentForPath(path));
            }
            existsCache.synchronous().invalidate(path);
            if (executor != null && updater != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Submitting reload cache task to the executor for path: {}, updater: {}", path, updater);
                }
                try {
                    executor.executeOrdered(path, new SafeRunnable() {
                        @Override
                        public void safeRun() {
                            updater.reloadCache(path);
                        }
                    });
                } catch (RejectedExecutionException e) {
                    // Ok, the service is shutting down
                    LOG.error("Failed to updated zk-cache {} on zk-watch {}", path, e.getMessage());
                }
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Cannot reload cache for path: {}, updater: {}", path, updater);
                }
            }
        }
    }

    public void invalidateAll() {
        invalidateAllData();
        invalidateAllChildren();
        invalidateAllExists();
    }

    private void invalidateAllExists() {
        existsCache.synchronous().invalidateAll();
    }

    public void invalidateAllData() {
        dataCache.synchronous().invalidateAll();
    }

    public void invalidateAllChildren() {
        childrenCache.synchronous().invalidateAll();
    }

    public void invalidateData(String path) {
        dataCache.synchronous().invalidate(path);
    }

    public void invalidateChildren(String path) {
        childrenCache.synchronous().invalidate(path);
    }

    private void invalidateExists(String path) {
        existsCache.synchronous().invalidate(path);
    }

    public void asyncInvalidate(String path) {
        backgroundExecutor.execute(() -> invalidate(path));
    }

    public int getZkOperationTimeoutSeconds() {
        return zkOperationTimeoutSeconds;
    }

    public void invalidate(final String path) {
        invalidateData(path);
        invalidateChildren(path);
        invalidateExists(path);
    }

    /**
     * Returns if the node at the given path exists in the cache
     *
     * @param path
     *            path of the node
     * @return true if node exists, false if it does not
     * @throws KeeperException
     * @throws InterruptedException
     */
    public boolean exists(final String path) throws KeeperException, InterruptedException {
        return exists(path, this);
    }

    private boolean exists(final String path, Watcher watcher) throws KeeperException, InterruptedException {
        return existsAsync(path, watcher).join();
    }

    @SuppressWarnings("deprecation")
    public CompletableFuture<Boolean> existsAsync(String path, Watcher watcher) {
        return existsCache.get(path, (p, executor) -> {
            ZooKeeper zk = zkSession.get();
            if (zk == null) {
                return FutureUtil.failedFuture(new IOException("ZK session not ready"));
            }

            CompletableFuture<Boolean> future = new CompletableFuture<>();
            zk.exists(path, watcher, (rc, path1, ctx, stat) -> {
                if (rc == Code.OK.intValue()) {
                    future.complete(true);
                } else if (rc == Code.NONODE.intValue()) {
                    future.complete(false);
                } else {
                    future.completeExceptionally(KeeperException.create(rc));
                }
            }, null);

            return future;
        });
    }

    /**
     * Simple ZooKeeperCache use this method to invalidate the cache entry on watch event w/o automatic reloading the
     * cache
     *
     * @param path
     * @param deserializer
     * @return
     * @throws Exception
     */
    public <T> Optional<T> getData(final String path, final Deserializer<T> deserializer) throws Exception {
        return getData(path, this, deserializer).map(e -> e.getKey());
    }

    public <T> Optional<Entry<T, Stat>> getEntry(final String path, final Deserializer<T> deserializer) throws Exception {
        return getData(path, this, deserializer);
    }

    public <T> CompletableFuture<Optional<Entry<T, Stat>>> getEntryAsync(final String path, final Deserializer<T> deserializer) {
        CompletableFuture<Optional<Entry<T, Stat>>> future = new CompletableFuture<>();
        getDataAsync(path, this, deserializer)
            .thenAccept(future::complete)
            .exceptionally(ex -> {
                asyncInvalidate(path);
                if (ex.getCause() instanceof NoNodeException) {
                    future.complete(Optional.empty());
                } else {
                    future.completeExceptionally(ex.getCause());
                }

                return null;
            });
        return future;
    }

    public <T> CompletableFuture<Optional<T>> getDataAsync(final String path, final Deserializer<T> deserializer) {
        CompletableFuture<Optional<T>> future = new CompletableFuture<>();
        getDataAsync(path, this, deserializer).thenAccept(data -> {
            future.complete(data.map(e -> e.getKey()));
        }).exceptionally(ex -> {
            asyncInvalidate(path);
            if (ex.getCause() instanceof NoNodeException) {
                future.complete(Optional.empty());
            } else {
                future.completeExceptionally(ex.getCause());
            }

            return null;
        });
        return future;
    }

    /**
     * Cache that implements automatic reloading on update will pass a different Watcher object to reload cache entry
     * automatically
     *
     * @param path
     * @param watcher
     * @param deserializer
     * @return
     * @throws Exception
     */
    public <T> Optional<Entry<T, Stat>> getData(final String path, final Watcher watcher,
            final Deserializer<T> deserializer) throws Exception {
        try {
            return getDataAsync(path, watcher, deserializer).get(this.zkOperationTimeoutSeconds, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            asyncInvalidate(path);
            Throwable cause = e.getCause();
            if (cause instanceof KeeperException) {
                throw (KeeperException) cause;
            } else if (cause instanceof InterruptedException) {
                LOG.warn("Time-out while fetching {} zk-data in {} sec", path, this.zkOperationTimeoutSeconds);
                throw (InterruptedException) cause;
            } else if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else {
                throw new RuntimeException(cause);
            }
        } catch (TimeoutException e) {
            LOG.warn("Time-out while fetching {} zk-data in {} sec", path, this.zkOperationTimeoutSeconds);
            asyncInvalidate(path);
            throw e;
        }
    }

    @SuppressWarnings({ "unchecked", "deprecation" })
    public <T> CompletableFuture<Optional<Entry<T, Stat>>> getDataAsync(final String path, final Watcher watcher,
            final Deserializer<T> deserializer) {
        checkNotNull(path);
        checkNotNull(deserializer);

        // refresh zk-cache entry in background if it's already expired
        checkAndRefreshExpiredEntry(path, deserializer);
        CompletableFuture<Optional<Entry<T,Stat>>> future = new CompletableFuture<>();
        dataCache.get(path, (p, executor) -> {
            // Return a future for the z-node to be fetched from ZK
            CompletableFuture<Pair<Entry<Object, Stat>, Long>> zkFuture = new CompletableFuture<>();

            // Broker doesn't restart on global-zk session lost: so handling unexpected exception
            try {
                this.zkSession.get().getData(path, watcher, (rc, path1, ctx, content, stat) -> {
                    if (rc == Code.OK.intValue()) {
                        try {
                            T obj = deserializer.deserialize(path, content);
                            // avoid using the zk-client thread to process the result
                            executor.execute(() -> zkFuture.complete(ImmutablePair
                                    .of(new SimpleImmutableEntry<Object, Stat>(obj, stat), System.nanoTime())));
                        } catch (Exception e) {
                            executor.execute(() -> zkFuture.completeExceptionally(e));
                        }
                    } else if (rc == Code.NONODE.intValue()) {
                        // Return null values for missing z-nodes, as this is not "exceptional" condition
                        executor.execute(() -> zkFuture.complete(null));
                    } else {
                        executor.execute(() -> zkFuture.completeExceptionally(KeeperException.create(rc)));
                    }
                }, null);
            } catch (Exception e) {
                LOG.warn("Failed to access zkSession for {} {}", path, e.getMessage(), e);
                zkFuture.completeExceptionally(e);
            }

            return zkFuture;
        }).thenAccept(result -> {
            if (result != null) {
                future.complete(Optional.of((Entry<T, Stat>) result.getLeft()));
            } else {
                future.complete(Optional.empty());
            }
        }).exceptionally(ex -> {
            future.completeExceptionally(ex);
            return null;
        });

        return future;
    }

    private <T> void checkAndRefreshExpiredEntry(String path, final Deserializer<T> deserializer) {
        CompletableFuture<Pair<Entry<Object, Stat>, Long>> result = dataCache.getIfPresent(path);
        if (result != null && result.isDone()) {
            Pair<Entry<Object, Stat>, Long> entryPair = result.getNow(null);
            if (entryPair != null && entryPair.getRight() != null) {
                if ((System.nanoTime() - entryPair.getRight()) > TimeUnit.SECONDS.toNanos(cacheExpirySeconds)) {
                    this.zkSession.get().getData(path, this, (rc, path1, ctx, content, stat) -> {
                        if (rc != Code.OK.intValue()) {
                            log.warn("Failed to refresh zookeeper-cache for {} due to {}", path, rc);
                            return;
                        }
                        try {
                            T obj = deserializer.deserialize(path, content);
                            dataCache.put(path, CompletableFuture.completedFuture(ImmutablePair
                                    .of(new SimpleImmutableEntry<Object, Stat>(obj, stat), System.nanoTime())));
                        } catch (Exception e) {
                            log.warn("Failed to refresh zookeeper-cache for {}", path, e);
                        }
                    }, null);
                }
            }
        }
    }

    /**
     * Simple ZooKeeperChildrenCache use this method to invalidate cache entry on watch event w/o automatic re-loading
     *
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public Set<String> getChildren(final String path) throws KeeperException, InterruptedException {
        try {
            return getChildrenAsync(path, this).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof KeeperException) {
                throw (KeeperException)e.getCause();
            } else {
                throw e;
            }
        }
    }

    /**
     * ZooKeeperChildrenCache implementing automatic re-loading on update use this method by passing in a different
     * Watcher object to reload cache entry
     *
     * @param path
     * @param watcher
     * @return
     */
    @SuppressWarnings("deprecation")
    public CompletableFuture<Set<String>> getChildrenAsync(String path, Watcher watcher) {
        return childrenCache.get(path, (p, executor) -> {
            CompletableFuture<Set<String>> future = new CompletableFuture<>();
            executor.execute(SafeRunnable.safeRun(() -> {
                ZooKeeper zk = zkSession.get();
                if (zk == null) {
                    future.completeExceptionally(new IOException("ZK session not ready"));
                    return;
                }

                zk.getChildren(path, watcher, (rc, path1, ctx, children) -> {
                    if (rc == Code.OK.intValue()) {
                        future.complete(Sets.newTreeSet(children));
                    } else if (rc == Code.NONODE.intValue()) {
                        // The node we want may not exist yet, so put a watcher on its existence
                        // before throwing up the exception. Its possible that the node could have
                        // been created after the call to getChildren, but before the call to exists().
                        // If this is the case, exists will return true, and we just call getChildren again.
                        existsAsync(path, watcher).thenAccept(exists -> {
                            if (exists) {
                                getChildrenAsync(path, watcher)
                                        .thenAccept(c -> future.complete(c))
                                        .exceptionally(ex -> {
                                            future.completeExceptionally(ex);
                                            return null;
                                        });
                            } else {
                                // Z-node does not exist
                                future.complete(Collections.emptySet());
                            }
                        }).exceptionally(ex -> {
                            future.completeExceptionally(ex);
                            return null;
                        });
                    } else {
                        future.completeExceptionally(KeeperException.create(rc));
                    }
                }, null);
            }));

            return future;
        });
    }

    @SuppressWarnings("unchecked")
    public <T> T getDataIfPresent(String path) {
        CompletableFuture<Pair<Entry<Object, Stat>, Long>> f = dataCache.getIfPresent(path);
        if (f != null && f.isDone() && !f.isCompletedExceptionally()) {
            return (T) f.join().getLeft().getKey();
        } else {
            return null;
        }
    }

    public Set<String> getChildrenIfPresent(String path) {
        CompletableFuture<Set<String>> future = childrenCache.getIfPresent(path);
        if (future != null && future.isDone() && !future.isCompletedExceptionally()) {
            return future.getNow(null);
        } else {
            return null;
        }
    }

    @Override
    public void process(WatchedEvent event) {
        LOG.info("[{}] Received ZooKeeper watch event: {}", zkSession.get(), event);
        this.process(event, null);
    }

    public void invalidateRoot(String root) {
        for (String key : childrenCache.synchronous().asMap().keySet()) {
            if (key.startsWith(root)) {
                childrenCache.synchronous().invalidate(key);
            }
        }
    }

    public void stop() {
        if (shouldShutdownExecutor) {
            this.executor.shutdown();
        }

        this.backgroundExecutor.shutdown();
    }

    public boolean checkRegNodeAndWaitExpired(String regPath) throws IOException {
        final CountDownLatch prevNodeLatch = new CountDownLatch(1);
        Watcher zkPrevRegNodewatcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // Check for prev znode deletion. Connection expiration is
                // not handling, since bookie has logic to shutdown.
                if (EventType.NodeDeleted == event.getType()) {
                    prevNodeLatch.countDown();
                }
            }
        };
        try {
            Stat stat = getZooKeeper().exists(regPath, zkPrevRegNodewatcher);
            if (null != stat) {
                // if the ephemeral owner isn't current zookeeper client
                // wait for it to be expired.
                if (stat.getEphemeralOwner() != getZooKeeper().getSessionId()) {
                    log.info("Previous bookie registration znode: {} exists, so waiting zk sessiontimeout:"
                        + " {} ms for znode deletion", regPath, getZooKeeper().getSessionTimeout());
                    // waiting for the previous bookie reg znode deletion
                    if (!prevNodeLatch.await(getZooKeeper().getSessionTimeout(), TimeUnit.MILLISECONDS)) {
                        throw new NodeExistsException(regPath);
                    } else {
                        return false;
                    }
                }
                return true;
            } else {
                return false;
            }
        } catch (KeeperException ke) {
            log.error("ZK exception checking and wait ephemeral znode {} expired : ", regPath, ke);
            throw new IOException("ZK exception checking and wait ephemeral znode "
                + regPath + " expired", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.error("Interrupted checking and wait ephemeral znode {} expired : ", regPath, ie);
            throw new IOException("Interrupted checking and wait ephemeral znode "
                + regPath + " expired", ie);
        }
    }

    private static Logger log = LoggerFactory.getLogger(ZooKeeperCache.class);
}
