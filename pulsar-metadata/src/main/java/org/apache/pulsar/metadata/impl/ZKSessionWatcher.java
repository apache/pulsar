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
import java.util.function.LongSupplier;
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
    private final LongSupplier nanoTime;

    private long disconnectedAt = 0;
    // A slow probe is not a disconnect, but its first timeout must count if a disconnect is later confirmed.
    private long firstUnansweredProbeAt = 0;
    private HandleIdentity probeHandleIdentity;
    // Recovery and confirmed disconnection invalidate callbacks from probes issued before that transition.
    private long probeGeneration = 0;
    private boolean closed;
    // Highest handle generation accepted as writable by the session watcher. A terminal event from an older handle
    // remains relevant until the watcher has processed a writable connection on a newer handle.
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
        this(zk, sessionListener, startScheduler, System::nanoTime);
    }

    @VisibleForTesting
    ZKSessionWatcher(ZooKeeper zk, Consumer<SessionEvent> sessionListener, boolean startScheduler,
                     LongSupplier nanoTime) {
        this.zk = zk;
        this.nanoTime = nanoTime;
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
        synchronized (this) {
            closed = true;
            resetProbeTiming();
        }
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
            Probe probe = beginProbe();
            if (probe == null) {
                return;
            }
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
                ZooKeeper.States clientState = zk.getState();
                stateSnapshot = new StateSnapshot(getKeeperState(clientState), clientState);
                checkStateIfSameSession(probe, stateSnapshot, true);
                // The response may arrive after this check returns. Clear only this outage's timing, without
                // notifying listeners on the ZooKeeper event thread or letting an old callback reset a newer outage.
                future.thenAccept(lateSnapshot -> {
                    if (lateSnapshot.keeperState() == KeeperState.SyncConnected
                            || lateSnapshot.keeperState() == KeeperState.ConnectedReadOnly) {
                        clearTimedOutProbe(probe);
                    }
                });
                return;
            }

            checkStateIfSameSession(probe, stateSnapshot, false);
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
        if (closed) {
            return;
        }
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
        if (!staleHandle && stateSnapshot.sessionId() == zk.getSessionId()) {
            trackProbeHandle(new HandleIdentity(stateSnapshot.handleGeneration(), stateSnapshot.sessionId()));
        }
        log.info().attr("event", stateSnapshot.event()).log("Got ZK session watch event");
        checkState(stateSnapshot.event().getState(), stateSnapshot.clientState(), stateSnapshot.sessionId(),
                stateSnapshot.handleGeneration(), sessionListener);
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
        resetProbeTiming();
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

    private synchronized Probe beginProbe() {
        HandleIdentity identity = getCurrentHandleIdentity();
        return runIfCurrentHandle(identity, () -> trackProbeHandle(identity))
                ? new Probe(identity, probeGeneration) : null;
    }

    private void trackProbeHandle(HandleIdentity identity) {
        if (!identity.equals(probeHandleIdentity)) {
            resetProbeTiming();
            probeHandleIdentity = identity;
        }
    }

    private void resetProbeTiming() {
        firstUnansweredProbeAt = 0;
        disconnectedAt = 0;
        probeGeneration++;
    }

    private synchronized void clearTimedOutProbe(Probe probe) {
        runIfCurrentHandle(probe.handleIdentity(), () -> {
            ZooKeeper.States state = zk.getState();
            if (probe.generation() == probeGeneration
                    && (replacedSessionId == 0 || replacedSessionId != probe.handleIdentity().sessionId())
                    && (state == ZooKeeper.States.CONNECTED || state == ZooKeeper.States.CONNECTEDREADONLY)) {
                resetProbeTiming();
            }
        });
    }

    // Called under the watcher monitor. Hold the client's handle-publication lock only for state bookkeeping;
    // listeners must run after that lock is released.
    private boolean runIfCurrentHandle(HandleIdentity checkedHandleIdentity, Runnable stateUpdate) {
        if (closed) {
            return false;
        }
        if (zk instanceof PulsarZooKeeperClient) {
            PulsarZooKeeperClient.CurrentHandleIdentity expectedIdentity =
                    new PulsarZooKeeperClient.CurrentHandleIdentity(checkedHandleIdentity.handleGeneration(),
                            checkedHandleIdentity.sessionId());
            return ((PulsarZooKeeperClient) zk).runStateUpdateIfCurrentHandle(expectedIdentity, stateUpdate);
        }
        if (!checkedHandleIdentity.equals(getCurrentHandleIdentity())) {
            return false;
        }
        stateUpdate.run();
        return true;
    }

    private synchronized void checkStateIfSameSession(Probe probe, StateSnapshot stateSnapshot, boolean timedOut) {
        if (closed) {
            return;
        }
        List<SessionEvent> notifications = new ArrayList<>(3);
        boolean processed = runIfCurrentHandle(probe.handleIdentity(), () -> {
            if (probe.generation() != probeGeneration) {
                return;
            }
            if (timedOut && stateSnapshot.clientState() == ZooKeeper.States.CONNECTED) {
                if (disconnectedAt != 0) {
                    handleDisconnected(notifications::add);
                } else if (firstUnansweredProbeAt == 0) {
                    firstUnansweredProbeAt = nanoTime.getAsLong();
                }
                return;
            }
            if (timedOut && stateSnapshot.clientState() == ZooKeeper.States.CLOSED && disconnectedAt != 0) {
                handleDisconnected(notifications::add);
                return;
            }
            checkState(stateSnapshot.keeperState(), stateSnapshot.clientState(), probe.handleIdentity().sessionId(),
                    probe.handleIdentity().handleGeneration(), notifications::add);
        });
        if (!processed) {
            logStaleProbe(probe.handleIdentity(), getCurrentHandleIdentity(), stateSnapshot);
            return;
        }
        notifySessionListener(notifications);
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
                                         long handleGeneration, Consumer<SessionEvent> notificationSink) {
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
            handleSyncConnected(sessionId, handleGeneration, notificationSink);
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
            disconnectedAt = firstUnansweredProbeAt != 0 ? firstUnansweredProbeAt : nanoTime.getAsLong();
            firstUnansweredProbeAt = 0;
            probeGeneration++;
        }

        long timeRemainingMillis = monitorTimeoutMillis
                - TimeUnit.NANOSECONDS.toMillis(nanoTime.getAsLong() - disconnectedAt);
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
        resetProbeTiming();
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

    private void handleSyncConnected(long sessionId, long handleGeneration, Consumer<SessionEvent> notificationSink) {
        if (replacedSessionId != 0 && sessionId == replacedSessionId) {
            log.info().attr("sessionId", replacedSessionId)
                    .log("Ignoring the connection of a ZooKeeper session that is being replaced");
            return;
        }
        // Even an already-established session can have unanswered probes whose timing must be discarded on recovery.
        resetProbeTiming();
        boolean newHandle = handleGeneration > lastConnectedHandleGeneration;
        if (!newHandle && currentStatus == SessionEvent.SessionReestablished) {
            return;
        }

        // A replacement handle has a new session. Its connection can reach this watcher before the old
        // handle's queued Expired event, which becomes stale once we advance the connected generation.
        boolean sessionAlreadyLost = currentStatus == SessionEvent.SessionLost;
        if (newHandle) {
            lastConnectedHandleGeneration = handleGeneration;
        }
        replacedSessionId = 0;

        log.info().attr("currentStatus", currentStatus).log("ZooKeeper client reconnection with server quorum");
        // Commit the state before notifying the listener, which can invalidate the new session reentrantly.
        currentStatus = SessionEvent.SessionReestablished;
        notifyReconnection(notificationSink, newHandle && !sessionAlreadyLost, newHandle || sessionAlreadyLost);
    }

    private static void notifyReconnection(Consumer<SessionEvent> listener, boolean notifyLoss,
                                           boolean notifyReestablishment) {
        try {
            if (notifyLoss) {
                listener.accept(SessionEvent.SessionLost);
            }
        } finally {
            try {
                listener.accept(SessionEvent.Reconnected);
            } finally {
                if (notifyReestablishment) {
                    listener.accept(SessionEvent.SessionReestablished);
                }
            }
        }
    }

    private void notifySessionListener(List<SessionEvent> notifications) {
        if (notifications.size() >= 2) {
            // Only a session reestablishment produces multiple notifications, with SessionLost first if needed.
            notifyReconnection(sessionListener, notifications.get(0) == SessionEvent.SessionLost, true);
        } else if (notifications.size() == 1) {
            sessionListener.accept(notifications.get(0));
        }
    }

    private record StateSnapshot(KeeperState keeperState, ZooKeeper.States clientState) {
    }

    private record HandleIdentity(long handleGeneration, long sessionId) {
    }

    private record Probe(HandleIdentity handleIdentity, long generation) {
    }

    record SessionEventSnapshot(WatchedEvent event, long handleGeneration, long sessionId,
                                ZooKeeper.States clientState) {
    }
}
