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

import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
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

    public ConsumerSession(String consumerName, long consumerId, TransportCnx cnx) {
        this.consumerName = consumerName;
        this.consumerId = consumerId;
        this.cnx = cnx;
        this.connected = cnx != null;
    }

    /**
     * Create a session for a consumer whose registration was loaded from the metadata store
     * on controller leader failover. Starts in "disconnected" state; a grace timer should be
     * attached immediately so the consumer is evicted if it does not reconnect in time.
     */
    public static ConsumerSession restored(String consumerName) {
        return new ConsumerSession(consumerName, -1L, null);
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
     * Mark the session as disconnected. The caller is responsible for scheduling the
     * grace-period eviction task via {@link #setGraceTimer(ScheduledFuture)}.
     */
    public synchronized void markDisconnected() {
        this.connected = false;
        this.cnx = null;
    }

    public synchronized void setGraceTimer(ScheduledFuture<?> timer) {
        cancelGraceTimer();
        this.graceTimer = timer;
    }

    public synchronized void cancelGraceTimer() {
        if (graceTimer != null) {
            graceTimer.cancel(false);
            graceTimer = null;
        }
    }

    /**
     * Push an assignment update to this consumer, if currently connected. The actual
     * wire push is wired up in the protocol-commands commit; at this stage the method
     * exists so {@link SubscriptionCoordinator} can call it.
     */
    public void sendAssignmentUpdate(ConsumerAssignment assignment) {
        // no-op until the protocol-commands commit wires in the wire protocol
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
