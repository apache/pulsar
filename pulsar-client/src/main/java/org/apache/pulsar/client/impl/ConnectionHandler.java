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
package org.apache.pulsar.client.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.HandlerState.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionHandler {
    private static final AtomicReferenceFieldUpdater<ConnectionHandler, ClientCnx> CLIENT_CNX_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ConnectionHandler.class, ClientCnx.class, "clientCnx");
    @SuppressWarnings("unused")
    private volatile ClientCnx clientCnx = null;

    protected final HandlerState state;
    protected final Backoff backoff;
    private static final AtomicLongFieldUpdater<ConnectionHandler> EPOCH_UPDATER = AtomicLongFieldUpdater
            .newUpdater(ConnectionHandler.class, "epoch");
    // Start with -1L because it gets incremented before sending on the first connection
    private volatile long epoch = -1L;
    protected volatile long lastConnectionClosedTimestamp = 0L;
    private final AtomicBoolean duringConnect = new AtomicBoolean(false);
    protected final int randomKeyForSelectConnection;

    interface Connection {

        /**
         * @apiNote If the returned future is completed exceptionally, reconnectLater will be called.
         */
        CompletableFuture<Void> connectionOpened(ClientCnx cnx);
        default void connectionFailed(PulsarClientException e) {
        }
    }

    protected Connection connection;

    protected ConnectionHandler(HandlerState state, Backoff backoff, Connection connection) {
        this.state = state;
        this.randomKeyForSelectConnection = state.client.getCnxPool().genRandomKeyToSelectCon();
        this.connection = connection;
        this.backoff = backoff;
        CLIENT_CNX_UPDATER.set(this, null);
    }

    protected void grabCnx() {
        if (!duringConnect.compareAndSet(false, true)) {
            log.info("[{}] [{}] Skip grabbing the connection since there is a pending connection",
                    state.topic, state.getHandlerName());
            return;
        }

        if (CLIENT_CNX_UPDATER.get(this) != null) {
            log.warn("[{}] [{}] Client cnx already set, ignoring reconnection request",
                    state.topic, state.getHandlerName());
            return;
        }

        if (!isValidStateForReconnection()) {
            // Ignore connection closed when we are shutting down
            log.info("[{}] [{}] Ignoring reconnection request (state: {})",
                    state.topic, state.getHandlerName(), state.getState());
            return;
        }

        try {
            state.client.getConnection(state.topic, randomKeyForSelectConnection) //
                    .thenCompose(cnx -> connection.connectionOpened(cnx)) //
                    .thenAccept(__ -> duringConnect.set(false))
                    .exceptionally(this::handleConnectionError);
        } catch (Throwable t) {
            log.warn("[{}] [{}] Exception thrown while getting connection: ", state.topic, state.getHandlerName(), t);
            reconnectLater(t);
        }
    }

    private Void handleConnectionError(Throwable exception) {
        try {
            log.warn("[{}] [{}] Error connecting to broker: {}",
                    state.topic, state.getHandlerName(), exception.getMessage());
            if (exception instanceof PulsarClientException) {
                connection.connectionFailed((PulsarClientException) exception);
            } else if (exception.getCause() instanceof PulsarClientException) {
                connection.connectionFailed((PulsarClientException) exception.getCause());
            } else {
                connection.connectionFailed(new PulsarClientException(exception));
            }
        } catch (Throwable throwable) {
            log.error("[{}] [{}] Unexpected exception after the connection",
                    state.topic, state.getHandlerName(), throwable);
        }

        reconnectLater(exception);
        return null;
    }

    void reconnectLater(Throwable exception) {
        CLIENT_CNX_UPDATER.set(this, null);
        duringConnect.set(false);
        if (!isValidStateForReconnection()) {
            log.info("[{}] [{}] Ignoring reconnection request (state: {})",
                    state.topic, state.getHandlerName(), state.getState());
            return;
        }
        long delayMs = backoff.next();
        log.warn("[{}] [{}] Could not get connection to broker: {} -- Will try again in {} s",
                state.topic, state.getHandlerName(),
                exception.getMessage(), delayMs / 1000.0);
        if (state.changeToConnecting()) {
            state.client.timer().newTimeout(timeout -> {
                log.info("[{}] [{}] Reconnecting after connection was closed", state.topic, state.getHandlerName());
                grabCnx();
            }, delayMs, TimeUnit.MILLISECONDS);
        } else {
            log.info("[{}] [{}] Ignoring reconnection request (state: {})",
                    state.topic, state.getHandlerName(), state.getState());
        }
    }

    public void connectionClosed(ClientCnx cnx) {
        lastConnectionClosedTimestamp = System.currentTimeMillis();
        duringConnect.set(false);
        state.client.getCnxPool().releaseConnection(cnx);
        if (CLIENT_CNX_UPDATER.compareAndSet(this, cnx, null)) {
            if (!isValidStateForReconnection()) {
                log.info("[{}] [{}] Ignoring reconnection request (state: {})",
                        state.topic, state.getHandlerName(), state.getState());
                return;
            }
            long delayMs = backoff.next();
            state.setState(State.Connecting);
            log.info("[{}] [{}] Closed connection {} -- Will try again in {} s",
                    state.topic, state.getHandlerName(), cnx.channel(),
                    delayMs / 1000.0);
            state.client.timer().newTimeout(timeout -> {
                log.info("[{}] [{}] Reconnecting after timeout", state.topic, state.getHandlerName());
                grabCnx();
            }, delayMs, TimeUnit.MILLISECONDS);
        }
    }

    protected void resetBackoff() {
        backoff.reset();
    }

    public ClientCnx cnx() {
        return CLIENT_CNX_UPDATER.get(this);
    }

    protected void setClientCnx(ClientCnx clientCnx) {
        CLIENT_CNX_UPDATER.set(this, clientCnx);
    }

    /**
     * Update the {@link ClientCnx} for the class, then increment and get the epoch value. Note that the epoch value is
     * currently only used by the {@link ProducerImpl}.
     * @param clientCnx - the new {@link ClientCnx}
     * @return the epoch value to use for this pair of {@link ClientCnx} and {@link ProducerImpl}
     */
    protected long switchClientCnx(ClientCnx clientCnx) {
        setClientCnx(clientCnx);
        return EPOCH_UPDATER.incrementAndGet(this);
    }

    private boolean isValidStateForReconnection() {
        State state = this.state.getState();
        switch (state) {
            case Uninitialized:
            case Connecting:
            case RegisteringSchema:
            case Ready:
                // Ok
                return true;

            case Closing:
            case Closed:
            case Failed:
            case ProducerFenced:
            case Terminated:
                return false;
        }
        return false;
    }

    public long getEpoch() {
        return epoch;
    }

    private static final Logger log = LoggerFactory.getLogger(ConnectionHandler.class);
}
