/*
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
package org.apache.pulsar.metadata.impl;

import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * Monitor the ZK session state every few seconds and send notifications.
 */
@Slf4j
public class ZKSessionWatcher implements AutoCloseable, Watcher {
    private final ZooKeeper zk;

    private volatile SessionEvent currentStatus;
    private final Consumer<SessionEvent> sessionListener;

    // Maximum time to wait for ZK session to be re-connected to quorum (set to 5/6 of SessionTimeout)
    private final long monitorTimeoutMillis;

    // Interval at which we check the state of the zk session (set to 1/15 of SessionTimeout)
    private final long tickTimeMillis;

    private final ScheduledExecutorService scheduler;
    private final ScheduledFuture<?> task;

    private volatile long disconnectedAt = 0;

    public ZKSessionWatcher(ZooKeeper zk, Consumer<SessionEvent> sessionListener) {
        this(zk, sessionListener, true);
    }

    public ZKSessionWatcher(ZooKeeper zk, Consumer<SessionEvent> sessionListener, boolean enableCheckConnectionStatus) {
        this.zk = zk;
        this.monitorTimeoutMillis = zk.getSessionTimeout() * 5 / 6;
        this.tickTimeMillis = zk.getSessionTimeout() / 15;
        this.sessionListener = sessionListener;

        if (enableCheckConnectionStatus) {
            this.scheduler = Executors
                    .newSingleThreadScheduledExecutor(new DefaultThreadFactory("metadata-store-zk-session-watcher"));
            this.task =
                    scheduler.scheduleAtFixedRate(catchingAndLoggingThrowables(this::checkConnectionStatus), tickTimeMillis,
                            tickTimeMillis,
                            TimeUnit.MILLISECONDS);
        } else {
            this.scheduler = null;
            this.task = null;
        }

        this.changeCurrentStatus(SessionEvent.SessionReestablished, "init status");
    }

    @Override
    public void close() throws Exception {
        if (task != null) {
            task.cancel(true);
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    // task that runs every TICK_TIME to check zk connection
    private synchronized void checkConnectionStatus() {
        try {
            CompletableFuture<Watcher.Event.KeeperState> future = new CompletableFuture<>();
            zk.exists("/", false, (StatCallback) (rc, path, ctx, stat) -> {
                switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    future.complete(Watcher.Event.KeeperState.Disconnected);
                    break;

                case SESSIONEXPIRED:
                    future.complete(Watcher.Event.KeeperState.Expired);
                    break;

                case OK:
                default:
                    future.complete(Watcher.Event.KeeperState.SyncConnected);
                }
            }, null);

            Watcher.Event.KeeperState zkClientState;
            try {
                zkClientState = future.get(tickTimeMillis, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                // Consider zk disconnection if zk operation takes more than TICK_TIME
                zkClientState = Watcher.Event.KeeperState.Disconnected;
            }

            checkState(zkClientState);
        } catch (RejectedExecutionException | InterruptedException e) {
            if (task != null) {
                task.cancel(true);
            }
        } catch (Throwable t) {
            log.warn("Error while checking ZK connection status", t);
        }
    }

    @Override
    public synchronized void process(WatchedEvent event) {
        log.info("Got ZK session watch event: {}", event);
        checkState(event.getState());
    }

    synchronized void setSessionInvalid() {
        changeCurrentStatus(SessionEvent.SessionLost, "invalid session");
    }

    private synchronized void changeCurrentStatus(SessionEvent targetStatus, String reason) {
        log.info("change current status from {} to {} reason: {}", this.currentStatus, targetStatus, reason);
        currentStatus = targetStatus;
    }

    private void checkState(Watcher.Event.KeeperState zkClientState) {
        switch (zkClientState) {
        case Expired:
            if (currentStatus != SessionEvent.SessionLost) {
                log.error("[0x{}] ZooKeeper session expired", Long.toHexString(zk.getSessionId()));
                changeCurrentStatus(SessionEvent.SessionLost, "zk session expired");
                sessionListener.accept(currentStatus);
            }
            break;

        case Disconnected:
            if (disconnectedAt == 0) {
                // this is the first disconnect event, we should monitor the time out from now, so we record the
                // time of disconnect
                disconnectedAt = System.nanoTime();
            }

            long timeRemainingMillis = monitorTimeoutMillis
                    - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - disconnectedAt);
            if (timeRemainingMillis <= 0 && currentStatus != SessionEvent.SessionLost) {
                log.error("[0x{}] ZooKeeper session reconnection timeout. Notifying session is lost.",
                        Long.toHexString(zk.getSessionId()));
                changeCurrentStatus(SessionEvent.SessionLost, "zk reconnection timeout");
                sessionListener.accept(currentStatus);
            } else if (currentStatus != SessionEvent.SessionLost) {
                log.warn("[0x{}] ZooKeeper client is disconnected. Waiting to reconnect, time remaining = {} seconds",
                        Long.toHexString(zk.getSessionId()), timeRemainingMillis / 1000.0);
                if (currentStatus == SessionEvent.SessionReestablished) {
                    changeCurrentStatus(SessionEvent.ConnectionLost, "zk client disconnected");
                    sessionListener.accept(currentStatus);
                }
            }
            break;

        case SyncConnected:
            if (currentStatus != SessionEvent.SessionReestablished) {
                // since it reconnected to zookeeper, we reset the disconnected time
                log.info("[0x{}] ZooKeeper client reconnection with server quorum. " +
                                "Current status: {}. Current zkClientState {}", Long.toHexString(zk.getSessionId()),
                        currentStatus, zkClientState);
                disconnectedAt = 0;

                sessionListener.accept(SessionEvent.Reconnected);
                if (currentStatus == SessionEvent.SessionLost) {
                    sessionListener.accept(SessionEvent.SessionReestablished);
                }
                changeCurrentStatus(SessionEvent.SessionReestablished, "session reconnected");
            }
            break;
        default:
            log.info("[0x{}] ZooKeeper client receive WatchedEvent[{}] " +
                            "Current status: {}. Current zkClientState {}", Long.toHexString(zk.getSessionId()),
                    zkClientState, currentStatus, zkClientState);
            break;
        }
    }
}
