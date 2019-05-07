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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Sets;

import java.nio.file.Paths;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NoNodeException;
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
 * @param <T>
 */
public abstract class ZooKeeperCache implements Watcher {
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

    protected final AsyncLoadingCache<String, Entry<Object, Stat>> dataCache;
    protected final Cache<String, Set<String>> childrenCache;
    protected final Cache<String, Boolean> existsCache;
    private final OrderedExecutor executor;
    private final OrderedExecutor backgroundExecutor = OrderedExecutor.newBuilder().name("zk-cache-background").numThreads(2).build();
    private boolean shouldShutdownExecutor;
    private final int zkOperationTimeoutSeconds;

    protected AtomicReference<ZooKeeper> zkSession = new AtomicReference<ZooKeeper>(null);

    public ZooKeeperCache(ZooKeeper zkSession, int zkOperationTimeoutSeconds, OrderedExecutor executor) {
        checkNotNull(executor);
        this.zkOperationTimeoutSeconds = zkOperationTimeoutSeconds;
        this.executor = executor;
        this.zkSession.set(zkSession);
        this.shouldShutdownExecutor = false;

        this.dataCache = Caffeine.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES)
                .buildAsync((key, executor1) -> null);

        this.childrenCache = CacheBuilder.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).build();
        this.existsCache = CacheBuilder.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).build();
    }

    public ZooKeeperCache(ZooKeeper zkSession, int zkOperationTimeoutSeconds) {
        this(zkSession, zkOperationTimeoutSeconds,
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
            childrenCache.invalidate(path);
            // sometimes zk triggers one watch per zk-session and if zkDataCache and ZkChildrenCache points to this
            // ZookeeperCache instance then ZkChildrenCache may not invalidate for it's parent. Therefore, invalidate
            // cache for parent if child is created/deleted
            if (event.getType().equals(EventType.NodeCreated) || event.getType().equals(EventType.NodeDeleted)) {
                childrenCache.invalidate(Paths.get(path).getParent().toString());
            }
            existsCache.invalidate(path);
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
        existsCache.invalidateAll();
    }

    public void invalidateAllData() {
        dataCache.synchronous().invalidateAll();
    }

    public void invalidateAllChildren() {
        childrenCache.invalidateAll();
    }

    public void invalidateData(String path) {
        dataCache.synchronous().invalidate(path);
    }

    public void invalidateChildren(String path) {
        childrenCache.invalidate(path);
    }

    private void invalidateExists(String path) {
        existsCache.invalidate(path);
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
        try {
            return existsCache.get(path, new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    return zkSession.get().exists(path, watcher) != null;
                }
            });
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof KeeperException) {
                throw (KeeperException) cause;
            } else if (cause instanceof InterruptedException) {
                throw (InterruptedException) cause;
            } else if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else {
                throw new RuntimeException(cause);
            }
        }
    }

    /**
     * Simple ZooKeeperCache use this method to invalidate the cache entry on watch event w/o automatic reloading the
     * cache
     *
     * @param path
     * @param deserializer
     * @param stat
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
     * @param stat
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

        CompletableFuture<Optional<Entry<T, Stat>>> future = new CompletableFuture<>();
        dataCache.get(path, (p, executor) -> {
            // Return a future for the z-node to be fetched from ZK
            CompletableFuture<Entry<Object, Stat>> zkFuture = new CompletableFuture<>();

            // Broker doesn't restart on global-zk session lost: so handling unexpected exception
            try {
                this.zkSession.get().getData(path, watcher, (rc, path1, ctx, content, stat) -> {
                    if (rc == Code.OK.intValue()) {
                        try {
                            T obj = deserializer.deserialize(path, content);
                            // avoid using the zk-client thread to process the result
                            executor.execute(
                                    () -> zkFuture.complete(new SimpleImmutableEntry<Object, Stat>(obj, stat)));
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
                future.complete(Optional.of((Entry<T, Stat>) result));
            } else {
                future.complete(Optional.empty());
            }
        }).exceptionally(ex -> {
            future.completeExceptionally(ex);
            return null;
        });

        return future;
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
        return getChildren(path, this);
    }

    /**
     * ZooKeeperChildrenCache implementing automatic re-loading on update use this method by passing in a different
     * Watcher object to reload cache entry
     *
     * @param path
     * @param watcher
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public Set<String> getChildren(final String path, final Watcher watcher)
            throws KeeperException, InterruptedException {
        try {
            return childrenCache.get(path, new Callable<Set<String>>() {
                @Override
                public Set<String> call() throws Exception {
                    LOG.debug("Fetching children at {}", path);
                    return Sets.newTreeSet(checkNotNull(zkSession.get()).getChildren(path, watcher));
                }
            });
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            // The node we want may not exist yet, so put a watcher on its existance
            // before throwing up the exception. Its possible that the node could have
            // been created after the call to getChildren, but before the call to exists().
            // If this is the case, exists will return true, and we just call getChildren again.
            if (cause instanceof KeeperException.NoNodeException
                    && exists(path, watcher)) {
                return getChildren(path, watcher);
            } else if (cause instanceof KeeperException) {
                throw (KeeperException) cause;
            } else if (cause instanceof InterruptedException) {
                throw (InterruptedException) cause;
            } else if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else {
                throw new RuntimeException(cause);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T getDataIfPresent(String path) {
        return (T) dataCache.getIfPresent(path);
    }

    public Set<String> getChildrenIfPresent(String path) {
        return childrenCache.getIfPresent(path);
    }

    @Override
    public void process(WatchedEvent event) {
        LOG.info("[{}] Received ZooKeeper watch event: {}", zkSession.get(), event);
        this.process(event, null);
    }

    public void invalidateRoot(String root) {
        for (String key : childrenCache.asMap().keySet()) {
            if (key.startsWith(root)) {
                childrenCache.invalidate(key);
            }
        }
    }

    public void stop() {
        if (shouldShutdownExecutor) {
            this.executor.shutdown();
        }

        this.backgroundExecutor.shutdown();
    }
}
