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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.pulsar.zookeeper.ZooKeeperCache.CacheUpdater;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * This class keeps a cache of the children for several z-nodes and updates it whenever it's changed on zookeeper.
 */
public class ZooKeeperChildrenCache implements Watcher, CacheUpdater<Set<String>> {
    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperChildrenCache.class);

    private final ZooKeeperCache cache;
    private final String path;
    private final List<ZooKeeperCacheListener<Set<String>>> listeners = Lists.newCopyOnWriteArrayList();
    private final AtomicBoolean isShutdown;

    public ZooKeeperChildrenCache(ZooKeeperCache cache, String path) {
        this.cache = cache;
        this.path = path;
        isShutdown = new AtomicBoolean(false);
    }

    public Set<String> get() throws KeeperException, InterruptedException {
        return get(this.path);
    }

    public Set<String> get(String path) throws KeeperException, InterruptedException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getChildren called at: {}", path);
        }

        Set<String> children = cache.getChildrenAsync(path, this).join();
        if (children == null) {
            throw KeeperException.create(KeeperException.Code.NONODE);
        }

        return children;
    }

    public CompletableFuture<Set<String>> getAsync() {
        return getAsync(this.path);
    }

    public CompletableFuture<Set<String>> getAsync(String path) {
        return cache.getChildrenAsync(path, this);
    }

    public void clear() {
        cache.invalidateChildren(path);
    }

    @Override
    public void reloadCache(final String path) {
        cache.invalidate(path);
        cache.getChildrenAsync(path, this)
                .thenAccept(children -> {
                    LOG.info("reloadCache called in zookeeperChildrenCache for path {}", path);
                    for (ZooKeeperCacheListener<Set<String>> listener : listeners) {
                        listener.onUpdate(path, children, null);
                    }
                }).exceptionally(ex -> {
                    LOG.warn("Reloading ZooKeeperDataCache failed at path:{}", path, ex);
                    return null;
                }).join();
    }

    @Override
    public void registerListener(ZooKeeperCacheListener<Set<String>> listener) {
        listeners.add(listener);
    }

    @Override
    public void unregisterListener(ZooKeeperCacheListener<Set<String>> listener) {
        listeners.remove(listener);
    }

    @Override
    public void process(WatchedEvent event) {
        LOG.info("[{}] Received ZooKeeper watch event: {}", cache.zkSession.get(), event);
        if (!isShutdown.get()) {
            cache.process(event, this);
        }
    }

    public void close() {
        isShutdown.set(true);
    }
}
