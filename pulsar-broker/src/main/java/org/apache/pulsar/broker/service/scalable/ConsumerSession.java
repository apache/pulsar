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
package org.apache.pulsar.broker.service.scalable;

import io.github.merlimat.slog.Logger;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import org.apache.pulsar.broker.service.TransportCnx;

/**
 * In-memory handle for a consumer registered with the controller leader.
 *
 * <p>Session identity is the stable {@code consumerName} chosen by the client.
 * This class wraps the <em>durable</em> portion (persisted via
 * {@link org.apache.pulsar.broker.resources.ConsumerRegistration}) with the
 * <em>transient</em> keep-alive state: the current transport connection, whether the
 * consumer is currently connected, and — when disconnected — a grace-period timer that
 * evicts the session if the consumer does not reconnect in time.
 *
 * <p>The keep-alive fields ({@code connected}, {@code consumerId}, {@code cnx},
 * {@code graceTimer}) are <em>not</em> persisted; they live on the controller leader only
 * and are reset to a "just disconnected" state when a new leader takes over.
 *
 * <p>Equality and hash are based on {@code consumerName} alone so that a reconnection
 * with a new protocol-level {@code consumerId} resolves to the same session.
 *
 * <p>The grace-period timer is fully encapsulated: {@link #markDisconnected()} schedules
 * it, {@link #attach(long, TransportCnx)} cancels it. The eviction action is supplied by
 * the caller as a {@link Runnable} at construction, so the session doesn't need a
 * back-reference to the coordinator.
 */
public class ConsumerSession {

    @Getter
    private final String consumerName;

    /** Current protocol-level consumer ID. Changes on each reconnect. */
    @Getter
    private volatile long consumerId;

    /** Current transport connection. Null when disconnected. */
    @Getter
    private volatile TransportCnx cnx;

    /** Whether the consumer is currently connected. */
    @Getter
    private volatile boolean connected;

    /** Scheduled eviction task, non-null only while disconnected and within grace period. */
    @Getter
    private volatile ScheduledFuture<?> graceTimer;

    private final Duration gracePeriod;
    private final ScheduledExecutorService scheduler;
    private final Runnable onGraceExpiry;
    private final Logger log;

    public ConsumerSession(String consumerName,
                           long consumerId,
                           TransportCnx cnx,
                           Duration gracePeriod,
                           ScheduledExecutorService scheduler,
                           Runnable onGraceExpiry,
                           Logger parentLogger) {
        this.consumerName = consumerName;
        this.consumerId = consumerId;
        this.cnx = cnx;
        this.connected = cnx != null;
        this.gracePeriod = gracePeriod;
        this.scheduler = scheduler;
        this.onGraceExpiry = onGraceExpiry;
        this.log = Logger.get(ConsumerSession.class).with()
                .ctx(parentLogger)
                .attr("consumerName", consumerName)
                .attr("consumerId", () -> this.consumerId)
                .attr("connected", () -> this.connected)
                .build();
    }

    /**
     * Create a session for a consumer whose registration was loaded from the metadata store
     * on controller leader failover. The returned session is in the "just disconnected"
     * state with its grace-period timer already armed — if the consumer does not reconnect
     * within the grace period the {@code onGraceExpiry} callback fires.
     */
    public static ConsumerSession restored(String consumerName,
                                           Duration gracePeriod,
                                           ScheduledExecutorService scheduler,
                                           Runnable onGraceExpiry,
                                           Logger parentLogger) {
        ConsumerSession session = new ConsumerSession(consumerName, -1L, null,
                gracePeriod, scheduler, onGraceExpiry, parentLogger);
        session.startGraceTimer();
        return session;
    }

    /**
     * Attach a new transport connection to this session (reconnect path). Cancels any
     * active grace timer and marks the session connected.
     */
    public synchronized void attach(long consumerId, TransportCnx cnx) {
        this.consumerId = consumerId;
        this.cnx = cnx;
        this.connected = true;
        cancelGraceTimer();
    }

    /**
     * Mark the session as disconnected and start the grace-period timer. The eviction task
     * (supplied at construction as {@code onGraceExpiry}) runs on the scheduler when the
     * timer fires unless a reconnect arrives first via {@link #attach(long, TransportCnx)}.
     */
    public synchronized void markDisconnected() {
        this.connected = false;
        this.cnx = null;
        log.info().attr("gracePeriodSeconds", gracePeriod.toSeconds())
                .log("Consumer disconnected; starting grace period");
        startGraceTimer();
    }

    /**
     * Start (or restart) the grace-period eviction timer using the configured
     * {@code onGraceExpiry} callback. Any previously-running timer is cancelled first.
     *
     * <p>Called from {@link #markDisconnected()} and from {@link #restored}.
     */
    private synchronized void startGraceTimer() {
        setGraceTimer(scheduler.schedule(onGraceExpiry,
                gracePeriod.toMillis(), TimeUnit.MILLISECONDS));
    }

    private synchronized void setGraceTimer(ScheduledFuture<?> timer) {
        cancelGraceTimer();
        this.graceTimer = timer;
    }

    /**
     * Cancel any pending grace timer. Package-private so that the coordinator can cancel
     * the timer when the consumer explicitly unregisters (in which case the session is
     * removed from the coordinator's map and no eviction callback should fire).
     */
    synchronized void cancelGraceTimer() {
        if (graceTimer != null) {
            graceTimer.cancel(false);
            graceTimer = null;
        }
    }

    /**
     * Push an assignment update to this consumer, if currently connected. Builds a
     * {@code ScalableConsumerAssignment} proto and writes a
     * {@code CommandScalableTopicAssignmentUpdate} to the connection.
     */
    public void sendAssignmentUpdate(ConsumerAssignment assignment) {
        TransportCnx localCnx = this.cnx;
        if (localCnx == null || !connected) {
            // Consumer is disconnected — no-op. The assignment will be delivered when it
            // reconnects (the coordinator re-pushes on attach).
            return;
        }
        var sender = localCnx.getCommandSender();
        if (sender == null) {
            // Connection is in the middle of being torn down; skip silently.
            return;
        }
        sender.sendScalableTopicAssignmentUpdate(consumerId, toProto(assignment));
    }

    /**
     * Convert the broker-side {@link ConsumerAssignment} record to its protocol wire form.
     * Shared by {@link #sendAssignmentUpdate(ConsumerAssignment)} and the
     * {@code handleCommandScalableTopicSubscribe} path in {@code ServerCnx}.
     */
    public static org.apache.pulsar.common.api.proto.ScalableConsumerAssignment toProto(
            ConsumerAssignment assignment) {
        var proto = new org.apache.pulsar.common.api.proto.ScalableConsumerAssignment()
                .setLayoutEpoch(assignment.layoutEpoch());
        for (ConsumerAssignment.AssignedSegment seg : assignment.assignedSegments()) {
            proto.addSegment()
                    .setSegmentId(seg.segmentId())
                    .setHashStart(seg.hashRange().start())
                    .setHashEnd(seg.hashRange().end())
                    .setSegmentTopic(seg.underlyingTopicName());
        }
        return proto;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConsumerSession that = (ConsumerSession) o;
        return Objects.equals(consumerName, that.consumerName);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(consumerName);
    }

    @Override
    public String toString() {
        return "ConsumerSession{name=" + consumerName + ", connected=" + connected + "}";
    }
}
