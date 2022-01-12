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
package org.apache.pulsar.metadata.impl;

import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.nio.charset.StandardCharsets;
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

/**
 * Monitor the ETCd session state every few seconds and send notifications.
 */
@Slf4j
public class EtcdSessionWatcher implements AutoCloseable {
    private final Client client;

    private SessionEvent currentStatus;
    private final Consumer<SessionEvent> sessionListener;

    // Maximum time to wait for Etcd lease to be re-connected to quorum (set to 5/6 of SessionTimeout)
    private final long monitorTimeoutMillis;

    // Interval at which we check the state of the Etcd connection (set to 1/15 of SessionTimeout)
    private final long tickTimeMillis;

    private final ScheduledExecutorService scheduler;
    private final ScheduledFuture<?> task;

    private long disconnectedAt = 0;

    public EtcdSessionWatcher(Client client, long sessionTimeoutMillis,
                              Consumer<SessionEvent> sessionListener) {
        this.client = client;
        this.monitorTimeoutMillis = sessionTimeoutMillis * 5 / 6;
        this.tickTimeMillis = sessionTimeoutMillis / 15;
        this.sessionListener = sessionListener;

        this.scheduler = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("metadata-store-etcd-session-watcher"));
        this.task =
                scheduler.scheduleAtFixedRate(catchingAndLoggingThrowables(this::checkConnectionStatus), tickTimeMillis,
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

    // task that runs every TICK_TIME to check Etcd connection
    private synchronized void checkConnectionStatus() {
        try {
            CompletableFuture<SessionEvent> future = new CompletableFuture<>();
            client.getKVClient().get(ByteSequence.from("/".getBytes(StandardCharsets.UTF_8)))
                    .thenRun(() -> {
                        future.complete(SessionEvent.Reconnected);
                    }).exceptionally(ex -> {
                        future.complete(SessionEvent.ConnectionLost);
                        return null;
                    });

            SessionEvent ectdClientState;
            try {
                ectdClientState = future.get(tickTimeMillis, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                // Consider etcd disconnection if etcd operation takes more than TICK_TIME
                ectdClientState = SessionEvent.ConnectionLost;
            }

            checkState(ectdClientState);
        } catch (RejectedExecutionException | InterruptedException e) {
            task.cancel(true);
        } catch (Throwable t) {
            log.warn("Error while checking Etcd connection status", t);
        }
    }

    synchronized void setSessionInvalid() {
        currentStatus = SessionEvent.SessionLost;
    }

    private void checkState(SessionEvent etcdlientState) {
        switch (etcdlientState) {
            case SessionLost:
                if (currentStatus != SessionEvent.SessionLost) {
                    log.error("Etcd lease has expired");
                    currentStatus = SessionEvent.SessionLost;
                    sessionListener.accept(currentStatus);
                }
                break;

            case ConnectionLost:
                if (disconnectedAt == 0) {
                    // this is the first disconnect event, we should monitor the time out from now, so we record the
                    // time of disconnect
                    disconnectedAt = System.nanoTime();
                }

                long timeRemainingMillis = monitorTimeoutMillis
                        - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - disconnectedAt);
                if (timeRemainingMillis <= 0 && currentStatus != SessionEvent.SessionLost) {
                    log.error("Etcd lease keep-alive timeout. Notifying session is lost.");
                    currentStatus = SessionEvent.SessionLost;
                    sessionListener.accept(currentStatus);
                } else if (currentStatus != SessionEvent.SessionLost) {
                    log.warn("Etcd client is disconnected. Waiting to reconnect, time remaining = {} seconds",
                            timeRemainingMillis / 1000.0);
                    if (currentStatus == SessionEvent.SessionReestablished) {
                        currentStatus = SessionEvent.ConnectionLost;
                        sessionListener.accept(currentStatus);
                    }
                }
                break;

            default:
                if (currentStatus != SessionEvent.SessionReestablished) {
                    // since it reconnected to Etcd, we reset the disconnected time
                    log.info("Etcd client reconnection with server quorum. Current status: {}", currentStatus);
                    disconnectedAt = 0;

                    sessionListener.accept(SessionEvent.Reconnected);
                    if (currentStatus == SessionEvent.SessionLost) {
                        sessionListener.accept(SessionEvent.SessionReestablished);
                    }
                    currentStatus = SessionEvent.SessionReestablished;
                }
                break;
        }
    }
}
