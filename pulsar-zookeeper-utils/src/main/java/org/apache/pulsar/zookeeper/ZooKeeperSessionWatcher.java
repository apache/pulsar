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

import java.io.Closeable;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * A ZooKeeper watcher for the Pulsar instance.
 *
 * Monitor the ZK session state every few seconds. If the session is not in Connected state for a while, we will kill
 * the process before the session is supposed to expire
 */
public class ZooKeeperSessionWatcher implements Watcher, StatCallback, Runnable, Closeable {

    public interface ShutdownService {
        default void run() {
            shutdown(0);
        }

        void shutdown(int exitCode);
    }

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperSessionWatcher.class);

    private final ZookeeperSessionExpiredHandler sessionExpiredHandler;
    private final ZooKeeper zk;
    // Maximum time to wait for ZK session to be re-connected to quorum (set to 5/6 of SessionTimeout)
    private final long monitorTimeoutMillis;
    // Interval at which we check the state of the zk session (set to 1/15 of SessionTimeout)
    private final long tickTimeMillis;

    private ScheduledExecutorService scheduler = null;
    private Watcher.Event.KeeperState keeperState = Watcher.Event.KeeperState.Disconnected;
    private long disconnectedAt = 0;
    private boolean shuttingDown = false;
    private volatile boolean zkOperationCompleted = false;
    private ScheduledFuture<?> task;

    public ZooKeeperSessionWatcher(ZooKeeper zk, long zkSessionTimeoutMillis, ZookeeperSessionExpiredHandler sessionExpiredHandler) {
        this.zk = zk;
        this.monitorTimeoutMillis = zkSessionTimeoutMillis * 5 / 6;
        this.tickTimeMillis = zkSessionTimeoutMillis / 15;
        this.sessionExpiredHandler = sessionExpiredHandler;
        this.sessionExpiredHandler.setWatcher(this);
    }

    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("pulsar-zk-session-watcher"));
        task = scheduler.scheduleAtFixedRate(this, tickTimeMillis, tickTimeMillis, TimeUnit.MILLISECONDS);
    }

    public Watcher.Event.KeeperState getKeeperState() {
        return this.keeperState;
    }

    // It is used only for testing purposes
    public boolean isShutdownStarted() {
        return shuttingDown;
    }

    @Override
    public void process(WatchedEvent event) {
        Watcher.Event.EventType eventType = event.getType();
        Watcher.Event.KeeperState eventState = event.getState();

        LOG.info("Received zookeeper notification, eventType={}, eventState={}", eventType, eventState);

        switch (eventType) {
        case None:
            if (eventState == Watcher.Event.KeeperState.Expired) {
                LOG.error("ZooKeeper session already expired, invoking shutdown");
                sessionExpiredHandler.onSessionExpired();
            }
            break;
        default:
            break;
        }

    }

    // callback for zk probing
    @Override
    public synchronized void processResult(int rc, String path, Object ctx, Stat stat) {
        switch (KeeperException.Code.get(rc)) {
        case CONNECTIONLOSS:
            keeperState = Watcher.Event.KeeperState.Disconnected;
            break;
        case SESSIONEXPIRED:
            keeperState = Watcher.Event.KeeperState.Expired;
            break;
        case OK:
        default:
            keeperState = Watcher.Event.KeeperState.SyncConnected;
        }
        zkOperationCompleted = true;
        this.notify();
    }

    // task that runs every TICK_TIME to check zk connection
    @Override
    public synchronized void run() {
        try {
            zkOperationCompleted = false;
            if (zk != null) {
                try {
                    zk.exists("/", false, this, null);
                    this.wait(tickTimeMillis);
                } catch (RejectedExecutionException | InterruptedException e) {
                    LOG.info("ZooKeeperSessionWatcher interrupted");
                    if (task != null) {
                        task.cancel(true);
                    }
                    return;
                }
            }
            if (!zkOperationCompleted) {
                // consider zk disconnection if zk operation takes more than TICK_TIME
                keeperState = Watcher.Event.KeeperState.Disconnected;
            }
            if (keeperState == Watcher.Event.KeeperState.Expired) {
                LOG.error("zookeeper session expired, invoking shutdown service");
                sessionExpiredHandler.onSessionExpired();
            } else if (keeperState == Watcher.Event.KeeperState.Disconnected) {
                if (disconnectedAt == 0) {
                    // this is the first disconnect, we should monitor the time out from now, so we record the time of
                    // disconnect
                    disconnectedAt = System.nanoTime();
                }

                long timeRemainingMillis = monitorTimeoutMillis
                        - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - disconnectedAt);
                if (timeRemainingMillis <= 0) {
                    LOG.error("timeout expired for reconnecting, invoking shutdown service");
                    sessionExpiredHandler.onSessionExpired();
                } else {
                    LOG.warn("zoo keeper disconnected, waiting to reconnect, time remaining = {} seconds",
                            TimeUnit.MILLISECONDS.toSeconds(timeRemainingMillis));
                }
            } else {
                if (disconnectedAt != 0) {
                    // since it reconnected to zoo keeper, we reset the disconnected time
                    LOG.info("reconnected to zoo keeper, system is back to normal.");
                    disconnectedAt = 0;
                }
            }
        } catch (Exception e) {
            LOG.warn(e.getMessage(), e);
        }
    }

    public void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        shuttingDown = true;
    }
}
