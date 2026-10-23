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
import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

/**
 * Monitor the ZK session state every few seconds and send notifications.
 */
@CustomLog
public class ZKSessionWatcher implements AutoCloseable, Watcher {
    static final long UNKNOWN_HANDLE_GENERATION = -1;
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
    // Highest handle generation whose SyncConnected event has been processed. A terminal event from an older handle
    // remains relevant until a newer handle has actually reached the session watcher, not merely been published.
    private long lastConnectedHandleGeneration = UNKNOWN_HANDLE_GENERATION;

    // The id of a session that a listener invalidated and that PulsarZooKeeperClient is replacing, or 0. Events of
    // that session, such as a connection check that still succeeds before the session is closed, must not
    // reestablish it, or the replacing session's events would find it reestablished already and send nothing.
    private long replacedSessionId = 0;

    public ZKSessionWatcher(ZooKeeper zk, Consumer<SessionEvent> sessionListener) {
        this(zk, sessionListener, true);
    }

    @VisibleForTesting
    ZKSessionWatcher(ZooKeeper zk, Consumer<SessionEvent> sessionListener, boolean startScheduler) {
        this.zk = zk;
        this.monitorTimeoutMillis = zk.getSessionTimeout() * 5 / 6;
        this.tickTimeMillis = zk.getSessionTimeout() / 15;
        this.sessionListener = sessionListener;
        this.lastConnectedHandleGeneration = zk instanceof PulsarZooKeeperClient
                ? ((PulsarZooKeeperClient) zk).getCurrentHandleGeneration()
                : UNKNOWN_HANDLE_GENERATION;

        this.scheduler = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("metadata-store-zk-session-watcher"));
        this.task = startScheduler
                ? scheduler.scheduleWithFixedDelay(
                        catchingAndLoggingThrowables(this::checkConnectionStatus), tickTimeMillis,
                        tickTimeMillis,
                        TimeUnit.MILLISECONDS)
                : null;
        this.currentStatus = SessionEvent.SessionReestablished;
    }

    @Override
    public void close() throws Exception {
        if (task != null) {
            task.cancel(true);
        }
        scheduler.shutdownNow();
        scheduler.awaitTermination(10, TimeUnit.SECONDS);
    }

    // Probe the current handle every TICK_TIME. The handle identity is checked atomically with the state update so a
    // delayed result cannot overwrite the state established by a replacement handle.
    @VisibleForTesting
    void checkConnectionStatus() {
        try {
            HandleIdentity checkedHandleIdentity = getCurrentHandleIdentity();
            CompletableFuture<StateSnapshot> future = new CompletableFuture<>();
            zk.exists("/", false, (StatCallback) (rc, path, ctx, stat) -> {
                ZooKeeper.States clientState = zk.getState();
                switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    future.complete(new StateSnapshot(KeeperState.Disconnected, clientState));
                    break;

                case SESSIONEXPIRED:
                    future.complete(new StateSnapshot(KeeperState.Expired, clientState));
                    break;

                case AUTHFAILED:
                    future.complete(new StateSnapshot(KeeperState.AuthFailed, clientState));
                    break;

                case OK:
                default:
                    future.complete(new StateSnapshot(getKeeperState(clientState), clientState));
                }
            }, null);

            StateSnapshot stateSnapshot;
            try {
                stateSnapshot = future.get(tickTimeMillis, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                // Consider zk disconnection if zk operation takes more than TICK_TIME
                stateSnapshot = new StateSnapshot(KeeperState.Disconnected, zk.getState());
            }

            checkStateIfSameSession(checkedHandleIdentity, stateSnapshot);
        } catch (RejectedExecutionException | InterruptedException e) {
            if (task != null) {
                task.cancel(true);
            }
        } catch (Throwable t) {
            log.warn().exception(t).log("Error while checking ZK connection status");
        }
    }

    @Override
    public void process(WatchedEvent event) {
        long handleGeneration = zk instanceof PulsarZooKeeperClient
                ? ((PulsarZooKeeperClient) zk).getCurrentHandleGeneration()
                : UNKNOWN_HANDLE_GENERATION;
        process(new SessionEventSnapshot(event, handleGeneration, zk.getSessionId(), zk.getState()));
    }

    synchronized void process(SessionEventSnapshot stateSnapshot) {
        long currentHandleGeneration = zk instanceof PulsarZooKeeperClient
                ? ((PulsarZooKeeperClient) zk).getCurrentHandleGeneration()
                : UNKNOWN_HANDLE_GENERATION;
        boolean staleHandle = stateSnapshot.handleGeneration() != UNKNOWN_HANDLE_GENERATION
                && currentHandleGeneration != UNKNOWN_HANDLE_GENERATION
                && stateSnapshot.handleGeneration() != currentHandleGeneration;
        if (staleHandle && (!isTerminalInvalidation(stateSnapshot)
                || stateSnapshot.handleGeneration() < lastConnectedHandleGeneration)) {
            log.debug()
                    .attr("event", stateSnapshot.event())
                    .attr("eventHandleGeneration", stateSnapshot.handleGeneration())
                    .attr("currentHandleGeneration", currentHandleGeneration)
                    .attr("eventSessionId", stateSnapshot.sessionId())
                    .attr("currentSessionId", zk.getSessionId())
                    .log("Ignoring ZooKeeper session event from a stale handle");
            return;
        }
        if (!staleHandle && stateSnapshot.event().getState() == KeeperState.SyncConnected
                && stateSnapshot.handleGeneration() != UNKNOWN_HANDLE_GENERATION) {
            lastConnectedHandleGeneration = Math.max(lastConnectedHandleGeneration,
                    stateSnapshot.handleGeneration());
        }
        log.info().attr("event", stateSnapshot.event()).log("Got ZK session watch event");
        checkState(stateSnapshot.event().getState(), stateSnapshot.clientState(), stateSnapshot.sessionId(),
                sessionListener);
    }

    private static boolean isTerminalInvalidation(SessionEventSnapshot stateSnapshot) {
        return stateSnapshot.event().getState() == KeeperState.Expired
                || (stateSnapshot.event().getState() == KeeperState.AuthFailed
                && stateSnapshot.clientState() == ZooKeeper.States.AUTH_FAILED);
    }

    private static KeeperState getKeeperState(ZooKeeper.States state) {
        if (state == null) {
            return KeeperState.Disconnected;
        }
        switch (state) {
        case CONNECTED:
            return KeeperState.SyncConnected;
        case CONNECTEDREADONLY:
            return KeeperState.ConnectedReadOnly;
        case AUTH_FAILED:
            return KeeperState.AuthFailed;
        case CLOSED:
            return KeeperState.Closed;
        case CONNECTING:
        case ASSOCIATING:
        case NOT_CONNECTED:
        default:
            return KeeperState.Disconnected;
        }
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

    private HandleIdentity getCurrentHandleIdentity() {
        if (zk instanceof PulsarZooKeeperClient) {
            PulsarZooKeeperClient.CurrentHandleIdentity identity =
                    ((PulsarZooKeeperClient) zk).getCurrentHandleIdentity();
            return new HandleIdentity(identity.generation(), identity.sessionId());
        }
        return new HandleIdentity(UNKNOWN_HANDLE_GENERATION, zk.getSessionId());
    }

    // Capture and compare the handle identity atomically with handle publication. The operation can be retried on a
    // replacement handle by PulsarZooKeeperClient, but its result is safe to use only while the captured identity is
    // still current. Keep this check and the state transition in the same synchronized section so a stale probe cannot
    // overwrite a newly established session.
    private synchronized void checkStateIfSameSession(HandleIdentity checkedHandleIdentity,
                                                       StateSnapshot stateSnapshot) {
        if (zk instanceof PulsarZooKeeperClient) {
            PulsarZooKeeperClient.CurrentHandleIdentity expectedIdentity =
                    new PulsarZooKeeperClient.CurrentHandleIdentity(checkedHandleIdentity.handleGeneration(),
                            checkedHandleIdentity.sessionId());
            List<SessionEvent> notifications = new ArrayList<>(2);
            boolean processed = ((PulsarZooKeeperClient) zk).runStateUpdateIfCurrentHandle(expectedIdentity,
                    () -> processCurrentProbeState(checkedHandleIdentity, stateSnapshot, notifications::add));
            if (!processed) {
                logStaleProbe(checkedHandleIdentity, getCurrentHandleIdentity(), stateSnapshot);
                return;
            }
            notifySessionListener(notifications);
            return;
        }

        HandleIdentity currentHandleIdentity = getCurrentHandleIdentity();
        if (checkedHandleIdentity.sessionId() != currentHandleIdentity.sessionId()) {
            logStaleProbe(checkedHandleIdentity, currentHandleIdentity, stateSnapshot);
            return;
        }
        processCurrentProbeState(checkedHandleIdentity, stateSnapshot, sessionListener);
    }

    private void processCurrentProbeState(HandleIdentity checkedHandleIdentity, StateSnapshot stateSnapshot,
                                          Consumer<SessionEvent> notificationSink) {
        if (stateSnapshot.keeperState() == KeeperState.SyncConnected
                && checkedHandleIdentity.handleGeneration() != UNKNOWN_HANDLE_GENERATION) {
            lastConnectedHandleGeneration = Math.max(lastConnectedHandleGeneration,
                    checkedHandleIdentity.handleGeneration());
        }
        checkState(stateSnapshot.keeperState(), stateSnapshot.clientState(), checkedHandleIdentity.sessionId(),
                notificationSink);
    }

    private void logStaleProbe(HandleIdentity checkedHandleIdentity, HandleIdentity currentHandleIdentity,
                               StateSnapshot stateSnapshot) {
        log.debug()
                .attr("checkedSessionId", checkedHandleIdentity.sessionId())
                .attr("currentSessionId", currentHandleIdentity.sessionId())
                .attr("checkedHandleGeneration", checkedHandleIdentity.handleGeneration())
                .attr("currentHandleGeneration", currentHandleIdentity.handleGeneration())
                .attr("zkClientState", stateSnapshot.keeperState())
                .log("Ignoring ZooKeeper session state from a stale ZooKeeper handle");
    }

    private synchronized void checkState(KeeperState zkClientState, ZooKeeper.States clientState, long sessionId,
                                         Consumer<SessionEvent> notificationSink) {
        switch (zkClientState) {
        case Expired:
            if (currentStatus != SessionEvent.SessionLost) {
                log.error("ZooKeeper session expired");
                currentStatus = SessionEvent.SessionLost;
                notificationSink.accept(currentStatus);
            }
            break;

        case Disconnected:
            handleDisconnected(notificationSink);
            break;

        case ConnectedReadOnly:
            handleConnectedReadOnly(notificationSink);
            break;

        case SyncConnected:
            handleSyncConnected(sessionId, notificationSink);
            break;

        case AuthFailed:
            if (clientState == ZooKeeper.States.AUTH_FAILED) {
                if (currentStatus != SessionEvent.SessionLost) {
                    log.error("ZooKeeper client authentication failed. Notifying session is lost.");
                    currentStatus = SessionEvent.SessionLost;
                    notificationSink.accept(currentStatus);
                }
            } else {
                log.debug().attr("currentStatus", currentStatus)
                        .log("ZooKeeper client authentication failed; waiting for the client connection state");
            }
            break;

        case SaslAuthenticated:
            log.info().attr("currentStatus", currentStatus)
                    .log("ZooKeeper client SASL authentication completed");
            break;

        case Closed:
            // A ZooKeeper handle was closed: the previous handle when PulsarZooKeeperClient replaces an expired
            // session with a new one, or the client when the metadata store closes. It says nothing about the
            // connection of the current handle, whose own events report the new session.
            break;

        default:
            log.warn().attr("zkClientState", zkClientState).attr("currentStatus", currentStatus)
                    .log("Ignoring ZooKeeper client state that does not indicate a reconnection");
            break;
        }
    }

    private void handleDisconnected(Consumer<SessionEvent> notificationSink) {
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
            notificationSink.accept(currentStatus);
        } else if (currentStatus != SessionEvent.SessionLost) {
            log.warn()
                    .attr("sessionId", zk.getSessionId())
                    .attr("timeRemainingSeconds", timeRemainingMillis / 1000.0)
                    .log("ZooKeeper client is disconnected. Waiting to reconnect");
            if (currentStatus == SessionEvent.SessionReestablished) {
                currentStatus = SessionEvent.ConnectionLost;
                notificationSink.accept(currentStatus);
            }
        }
    }

    private void handleConnectedReadOnly(Consumer<SessionEvent> notificationSink) {
        disconnectedAt = 0;
        if (currentStatus == SessionEvent.SessionReestablished) {
            log.warn()
                    .attr("sessionId", zk.getSessionId())
                    .log("ZooKeeper client is connected to a read-only server. Waiting for read-write connection");
            currentStatus = SessionEvent.ConnectionLost;
            notificationSink.accept(currentStatus);
        } else {
            log.debug().attr("sessionId", zk.getSessionId()).attr("currentStatus", currentStatus)
                    .log("ZooKeeper client remains connected to a read-only server");
        }
    }

    private void handleSyncConnected(long sessionId, Consumer<SessionEvent> notificationSink) {
        if (currentStatus != SessionEvent.SessionReestablished) {
            if (replacedSessionId != 0 && sessionId == replacedSessionId) {
                log.info().attr("sessionId", replacedSessionId)
                        .log("Ignoring the connection of a ZooKeeper session that is being replaced");
                return;
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
                notificationSink.accept(SessionEvent.Reconnected);
            } finally {
                if (previousStatus == SessionEvent.SessionLost) {
                    notificationSink.accept(SessionEvent.SessionReestablished);
                }
            }
        }
    }

    private void notifySessionListener(List<SessionEvent> notifications) {
        if (notifications.size() == 2) {
            try {
                sessionListener.accept(notifications.get(0));
            } finally {
                sessionListener.accept(notifications.get(1));
            }
        } else if (notifications.size() == 1) {
            sessionListener.accept(notifications.get(0));
        }
    }

    private record StateSnapshot(KeeperState keeperState, ZooKeeper.States clientState) {
    }

    private record HandleIdentity(long handleGeneration, long sessionId) {
    }

    record SessionEventSnapshot(WatchedEvent event, long handleGeneration, long sessionId,
                                ZooKeeper.States clientState) {
    }
}
