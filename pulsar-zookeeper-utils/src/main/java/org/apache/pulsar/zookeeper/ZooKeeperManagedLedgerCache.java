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

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class ZooKeeperManagedLedgerCache implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperManagedLedgerCache.class);

    private final ZooKeeperCache cache;
    private final String path;

    public ZooKeeperManagedLedgerCache(ZooKeeperCache cache, String path) {
        this.cache = cache;
        this.path = path;
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

    public CompletableFuture<Set<String>> getAsync(String path) {
        return cache.getChildrenAsync(path, this);
    }

    public void clearTree() {
        cache.invalidateRoot(path);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        LOG.info("[{}] Received ZooKeeper watch event: {}", cache.zkSession.get(), watchedEvent);
        String watchedEventPath = watchedEvent.getPath();
        if (watchedEventPath != null) {
            LOG.info("invalidate called in zookeeperChildrenCache for path {}", watchedEventPath);
            cache.invalidate(watchedEventPath);
        }
    }
}
