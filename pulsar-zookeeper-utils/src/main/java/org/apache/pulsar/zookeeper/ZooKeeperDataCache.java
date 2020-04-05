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

import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.pulsar.zookeeper.ZooKeeperCache.CacheUpdater;
import org.apache.pulsar.zookeeper.ZooKeeperCache.Deserializer;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Provides a generic cache for reading objects stored in zookeeper.
 *
 * Maintains the objects already serialized in memory and sets watches to receive changes notifications.
 *
 * @param <T>
 */
public abstract class ZooKeeperDataCache<T> implements Deserializer<T>, CacheUpdater<T>, Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperDataCache.class);

    private final ZooKeeperCache cache;
    private final List<ZooKeeperCacheListener<T>> listeners = Lists.newCopyOnWriteArrayList();
    private final int zkOperationTimeoutSeconds;

    private static final int FALSE = 0;
    private static final int TRUE = 1;

    private static final AtomicIntegerFieldUpdater<ZooKeeperDataCache> IS_SHUTDOWN_UPDATER = AtomicIntegerFieldUpdater
            .newUpdater(ZooKeeperDataCache.class, "isShutdown");
    private volatile int isShutdown = FALSE;

    public ZooKeeperDataCache(final ZooKeeperCache cache) {
        this.cache = cache;
        this.zkOperationTimeoutSeconds = cache.getZkOperationTimeoutSeconds();
    }

    public CompletableFuture<Optional<T>> getAsync(String path) {
        CompletableFuture<Optional<T>> future = new CompletableFuture<>();
        cache.getDataAsync(path, this, this).thenAccept(entry -> {
            future.complete(entry.map(Entry::getKey));
        }).exceptionally(ex -> {
            cache.asyncInvalidate(path);
            future.completeExceptionally(ex);
            return null;
        });

        return future;
    }

    public CompletableFuture<Optional<Entry<T, Stat>>> getWithStatAsync(String path) {
        return cache.getDataAsync(path, this, this).whenComplete((entry, ex) -> {
            if (ex != null) {
                cache.asyncInvalidate(path);
            }
        });
    }

    /**
     * Return an item from the cache
     *
     * If node doens't exist, the value will be not present.s
     *
     * @param path
     * @return
     * @throws Exception
     */
    public Optional<T> get(final String path) throws Exception {
        try {
            return getAsync(path).get(zkOperationTimeoutSeconds, TimeUnit.SECONDS);    
        }catch(TimeoutException e) {
            cache.asyncInvalidate(path);
            throw e;
        }
    }

    public Optional<Entry<T, Stat>> getWithStat(final String path) throws Exception {
        return cache.getData(path, this, this);
    }

    /**
     * Only for UTs (for now), as this clears the whole ZK data cache.
     */
    public void clear() {
        cache.invalidateAllData();
    }

    public void invalidate(final String path) {
        cache.invalidateData(path);
    }

    @Override
    public void reloadCache(final String path) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Reloading ZooKeeperDataCache at path {}", path);
        }
        cache.invalidate(path);

        cache.getDataAsync(path, this, this).thenAccept(cacheEntry -> {
            if (!cacheEntry.isPresent()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Node [{}] does not exist", path);
                }
                return;
            }

            for (ZooKeeperCacheListener<T> listener : listeners) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Notifying listener {} at path {}", listener, path);
                }
                listener.onUpdate(path, cacheEntry.get().getKey(), cacheEntry.get().getValue());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Notified listener {} at path {}", listener, path);
                }
            }
        }).exceptionally(ex -> {
            LOG.warn("Reloading ZooKeeperDataCache failed at path: {}", path, ex);
            return null;
        });
    }

    @Override
    public void registerListener(ZooKeeperCacheListener<T> listener) {
        listeners.add(listener);
    }

    @Override
    public void unregisterListener(ZooKeeperCacheListener<T> listener) {
        listeners.remove(listener);
    }

    @Override
    public void process(WatchedEvent event) {
        LOG.info("[{}] Received ZooKeeper watch event: {}", cache.zkSession.get(), event);
        if (IS_SHUTDOWN_UPDATER.get(this) == FALSE) {
            cache.process(event, this);
        }
    }

    public T getDataIfPresent(String path) {
        return (T) cache.getDataIfPresent(path);
    }


    public void close() {
        IS_SHUTDOWN_UPDATER.set(this, TRUE);
    }
}
