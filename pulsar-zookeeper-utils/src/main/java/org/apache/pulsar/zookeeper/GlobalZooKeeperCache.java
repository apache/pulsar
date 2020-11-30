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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory.SessionType;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per ZK client ZooKeeper cache supporting ZNode data and children list caches. A cache entry is identified, accessed
 * and invalidated by the ZNode path. For the data cache, ZNode data parsing is done at request time with the given
 * {@link Deserializer} argument.
 *
 */
public class GlobalZooKeeperCache extends ZooKeeperCache implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(GlobalZooKeeperCache.class);

    private final ZooKeeperClientFactory zlClientFactory;
    private final int zkSessionTimeoutMillis;
    private final String globalZkConnect;
    private final ScheduledExecutorService scheduledExecutor;

    public GlobalZooKeeperCache(ZooKeeperClientFactory zkClientFactory, int zkSessionTimeoutMillis,
            int zkOperationTimeoutSeconds, String globalZkConnect, OrderedExecutor orderedExecutor,
            ScheduledExecutorService scheduledExecutor, int cacheExpirySeconds) {
        super("global-zk", null, zkOperationTimeoutSeconds, orderedExecutor, cacheExpirySeconds);
        this.zlClientFactory = zkClientFactory;
        this.zkSessionTimeoutMillis = zkSessionTimeoutMillis;
        this.globalZkConnect = globalZkConnect;
        this.scheduledExecutor = scheduledExecutor;
    }

    public void start() throws IOException {
        CompletableFuture<ZooKeeper> zkFuture = zlClientFactory.create(globalZkConnect, SessionType.AllowReadOnly,
                zkSessionTimeoutMillis);

        // Initial session creation with global ZK must work
        try {
            ZooKeeper newSession = zkFuture.get(zkSessionTimeoutMillis, TimeUnit.MILLISECONDS);
            // Register self as a watcher to receive notification when session expires and trigger a new session to be
            // created
            newSession.register(this);
            zkSession.set(newSession);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOG.error("Failed to establish global zookeeper session: {}", e.getMessage(), e);
            throw new IOException(e);
        }
    }

    public void close() throws IOException {
        ZooKeeper currentSession = zkSession.getAndSet(null);
        if (currentSession != null) {
            try {
                currentSession.close();
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }

        super.stop();
    }

    @Override
    public <T> void process(WatchedEvent event, final CacheUpdater<T> updater) {
        synchronized (this) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("[{}] Got Global ZooKeeper WatchdEvent: EventType: {}, KeeperState: {}, path: {}",
                        this.hashCode(), event.getType(), event.getState(), event.getPath());
            }
            if (event.getType() == Event.EventType.None) {
                switch (event.getState()) {
                case Expired:
                    // in case of expired, the zkSession is no longer good for sure.
                    // We need to restart the session immediately.
                    // cancel any timer event since it is already bad
                    ZooKeeper oldSession = this.zkSession.getAndSet(null);
                    LOG.warn("Global ZK session lost. Triggering reconnection {}", oldSession);
                    safeCloseZkSession(oldSession);
                    asyncRestartZooKeeperSession();
                    return;
                case SyncConnected:
                case ConnectedReadOnly:
                    checkNotNull(zkSession.get());
                    LOG.info("Global ZK session {} restored connection.", zkSession.get());

                    //
                    dataCache.synchronous().invalidateAll();
                    childrenCache.synchronous().invalidateAll();
                    return;
                default:
                    break;
                }
            }
        }

        // Other types of events
        super.process(event, updater);

    }

    protected void asyncRestartZooKeeperSession() {
        // schedule a re-connect event using this as the watch
        LOG.info("Re-starting global ZK session in the background...");

        CompletableFuture<ZooKeeper> zkFuture = zlClientFactory.create(globalZkConnect, SessionType.AllowReadOnly,
                zkSessionTimeoutMillis);
        zkFuture.thenAccept(zk -> {
            if (zkSession.compareAndSet(null, zk)) {
                LOG.info("Successfully re-created global ZK session: {}", zk);
            } else {
                // Other reconnection happened, we can close the session
                safeCloseZkSession(zk);
            }
        }).exceptionally(ex -> {
            LOG.warn("Failed to re-create global ZK session: {}", ex.getMessage());

            // Schedule another attempt for later
            scheduledExecutor.schedule(() -> {
                asyncRestartZooKeeperSession();
            }, 10, TimeUnit.SECONDS);
            return null;
        });
    }

    private void safeCloseZkSession(ZooKeeper zkSession) {
        if (zkSession != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Closing global zkSession:{}", zkSession.getSessionId());
            }
            try {
                zkSession.close();
            } catch (Exception e) {
                LOG.info("Closing Global ZK Session encountered an exception: {}. Disposed anyway.", e.getMessage());
            }
        }
    }
}
