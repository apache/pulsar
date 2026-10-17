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
import lombok.CustomLog;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * Monitor the ZK session state every few seconds and send notifications.
 */
@CustomLog
public class ZKSessionWatcher implements AutoCloseable, Watcher {
    private final ZooKeeper zk;

    private SessionEvent currentStatus;
    private final Consumer<SessionEvent> sessionListener;

    // Maximum time to wait for ZK session to be re-connected to quorum (set to 5/6 of SessionTimeout)
    private final long monitorTimeoutMillis;

    // Interval at which we check the state of the zk session (set to 1/15 of SessionTimeout)
    private final long tickTimeMillis;

    private final ScheduledExecutorService scheduler;
    private final ScheduledFuture<?> task;

    private long disconnectedAt = 0;

    // The id of a session that a listener invalidated and that PulsarZooKeeperClient is replacing, or 0. Events of
    // that session, such as a connection check that still succeeds before the session is closed, must not
    // reestablish it, or the replacing session's events would find it reestablished already and send nothing.
    private long replacedSessionId = 0;

    public ZKSessionWatcher(ZooKeeper zk, Consumer<SessionEvent> sessionListener) {
        this.zk = zk;
        this.monitorTimeoutMillis = zk.getSessionTimeout() * 5 / 6;
        this.tickTimeMillis = zk.getSessionTimeout() / 15;
        this.sessionListener = sessionListener;

        this.scheduler = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("metadata-store-zk-session-watcher"));
        this.task =
                scheduler.scheduleWithFixedDelay(
                        catchingAndLoggingThrowables(this::checkConnectionStatus), tickTimeMillis,
                        tickTimeMillis,
                        TimeUnit.MILLISECONDS);
        this.currentStatus = SessionEvent.SessionReestablished;
    }

    @Override
    public void close() throws Exception {
        task.cancel(true);
        scheduler.shutdownNow();
        scheduler.awaitTermination(10, TimeUnit.SECONDS);
    }

    // task that runs every TICK_TIME to check zk connection
    // NOT ThreadSafe:
    // If zk client can't ensure the order, it may lead to problems.
    // Currently,we only use it in single thread, it will be fine. but we shouldn't leave any potential problems
    // in the future.
    private void checkConnectionStatus() {
        try {
            long checkedSessionId = zk.getSessionId();
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

            checkStateIfSameSession(checkedSessionId, zkClientState);
        } catch (RejectedExecutionException | InterruptedException e) {
            task.cancel(true);
        } catch (Throwable t) {
            log.warn().exception(t).log("Error while checking ZK connection status");
        }
    }

    @Override
    public synchronized void process(WatchedEvent event) {
        log.info().attr("event", event).log("Got ZK session watch event");
        checkState(event.getState());
    }

    synchronized void setSessionInvalid() {
        currentStatus = SessionEvent.SessionLost;
    }

    /**
     * Mark the current session invalid while it's being replaced with a new session: only the new session's
     * connection reestablishes the session.
     */
    synchronized void setSessionReplaced() {
        setSessionInvalid();
        replacedSessionId = zk.getSessionId();
    }

    // PulsarZooKeeperClient publishes the new ZooKeeper instance before forwarding the corresponding session event to
    // watcherManager, so zk.set(newZk) happens-before this watcher observes the new-session event. Keep the session-id
    // check and state transition in the same synchronized section to prevent stale async probes from racing with that
    // event and overwriting the state of the newly established session.
    private synchronized void checkStateIfSameSession(long checkedSessionId,
                                                      Watcher.Event.KeeperState zkClientState) {
        long currentSessionId = zk.getSessionId();
        if (checkedSessionId != currentSessionId) {
            log.warn()
                    .attr("checkedSessionId", checkedSessionId)
                    .attr("currentSessionId", currentSessionId)
                    .attr("zkClientState", zkClientState)
                    .log("Ignoring ZooKeeper session state from a stale session");
            return;
        }
        checkState(zkClientState);
    }

    private synchronized void checkState(Watcher.Event.KeeperState zkClientState) {
        switch (zkClientState) {
        case Expired:
            if (currentStatus != SessionEvent.SessionLost) {
                log.error("ZooKeeper session expired");
                currentStatus = SessionEvent.SessionLost;
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
                log.error("ZooKeeper session reconnection timeout. Notifying session is lost.");
                currentStatus = SessionEvent.SessionLost;
                sessionListener.accept(currentStatus);
            } else if (currentStatus != SessionEvent.SessionLost) {
                log.warn()
                        .attr("sessionId", zk.getSessionId())
                        .attr("timeRemainingSeconds", timeRemainingMillis / 1000.0)
                        .log("ZooKeeper client is disconnected. Waiting to reconnect");
                if (currentStatus == SessionEvent.SessionReestablished) {
                    currentStatus = SessionEvent.ConnectionLost;
                    sessionListener.accept(currentStatus);
                }
            }
            break;

        case Closed:
            // A ZooKeeper handle was closed: the previous handle when PulsarZooKeeperClient replaces an expired
            // session with a new one, or the client when the metadata store closes. It says nothing about the
            // connection of the current handle, whose own events report the new session.
            break;

        default:
            if (currentStatus != SessionEvent.SessionReestablished) {
                if (replacedSessionId != 0 && zk.getSessionId() == replacedSessionId) {
                    log.info().attr("sessionId", replacedSessionId)
                            .log("Ignoring the connection of a ZooKeeper session that is being replaced");
                    break;
                }
                replacedSessionId = 0;
                // since it reconnected to zoo keeper, we reset the disconnected time
                log.info().attr("currentStatus", currentStatus).log("ZooKeeper client reconnection with server quorum");
                disconnectedAt = 0;

                SessionEvent previousStatus = currentStatus;
                // Update the status before notifying the listener: a listener that can't use the new session, such
                // as ZKMetadataStore when it fails to recreate its persistent watch, calls setSessionReplaced() or
                // setSessionInvalid() while it's notified, and the status it sets must not be overwritten, or the next
                // session would not send SessionReestablished again.
                currentStatus = SessionEvent.SessionReestablished;
                try {
                    sessionListener.accept(SessionEvent.Reconnected);
                } finally {
                    if (previousStatus == SessionEvent.SessionLost) {
                        sessionListener.accept(SessionEvent.SessionReestablished);
                    }
                }
            }
            break;
        }
    }
}
