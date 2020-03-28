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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.google.common.annotations.VisibleForTesting;
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
    protected long epoch = 0L;

    interface Connection {
        void connectionFailed(PulsarClientException exception);
        void connectionOpened(ClientCnx cnx);
    }

    protected Connection connection;

    protected ConnectionHandler(HandlerState state, Backoff backoff, Connection connection) {
        this.state = state;
        this.connection = connection;
        this.backoff = backoff;
        CLIENT_CNX_UPDATER.set(this, null);
    }

    protected void grabCnx() {
        if (CLIENT_CNX_UPDATER.get(this) != null) {
            log.warn("[{}] [{}] Client cnx already set, ignoring reconnection request", state.topic, state.getHandlerName());
            return;
        }

        if (!isValidStateForReconnection()) {
            // Ignore connection closed when we are shutting down
            log.info("[{}] [{}] Ignoring reconnection request (state: {})", state.topic, state.getHandlerName(), state.getState());
            return;
        }

        try {
            state.client.getConnection(state.topic) //
                    .thenAccept(cnx -> connection.connectionOpened(cnx)) //
                    .exceptionally(this::handleConnectionError);
        } catch (Throwable t) {
            log.warn("[{}] [{}] Exception thrown while getting connection: ", state.topic, state.getHandlerName(), t);
            reconnectLater(t);
        }
    }

    private Void handleConnectionError(Throwable exception) {
        log.warn("[{}] [{}] Error connecting to broker: {}", state.topic, state.getHandlerName(), exception.getMessage());
        if (exception instanceof PulsarClientException) {
            connection.connectionFailed((PulsarClientException) exception);
        } else if (exception.getCause() instanceof  PulsarClientException) {
            connection.connectionFailed((PulsarClientException)exception.getCause());
        } else {
            connection.connectionFailed(new PulsarClientException(exception));
        }

        State state = this.state.getState();
        if (state == State.Uninitialized || state == State.Connecting || state == State.Ready) {
            reconnectLater(exception);
        }

        return null;
    }

    protected void reconnectLater(Throwable exception) {
        CLIENT_CNX_UPDATER.set(this, null);
        if (!isValidStateForReconnection()) {
            log.info("[{}] [{}] Ignoring reconnection request (state: {})", state.topic, state.getHandlerName(), state.getState());
            return;
        }
        long delayMs = backoff.next();
        log.warn("[{}] [{}] Could not get connection to broker: {} -- Will try again in {} s", state.topic, state.getHandlerName(),
                exception.getMessage(), delayMs / 1000.0);
        state.setState(State.Connecting);
        state.client.timer().newTimeout(timeout -> {
            log.info("[{}] [{}] Reconnecting after connection was closed", state.topic, state.getHandlerName());
            ++epoch;
            grabCnx();
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    public void connectionClosed(ClientCnx cnx) {
        if (CLIENT_CNX_UPDATER.compareAndSet(this, cnx, null)) {
            if (!isValidStateForReconnection()) {
                log.info("[{}] [{}] Ignoring reconnection request (state: {})", state.topic, state.getHandlerName(), state.getState());
                return;
            }
            long delayMs = backoff.next();
            state.setState(State.Connecting);
            log.info("[{}] [{}] Closed connection {} -- Will try again in {} s", state.topic, state.getHandlerName(), cnx.channel(),
                    delayMs / 1000.0);
            state.client.timer().newTimeout(timeout -> {
                log.info("[{}] [{}] Reconnecting after timeout", state.topic, state.getHandlerName());
                ++epoch;
                grabCnx();
            }, delayMs, TimeUnit.MILLISECONDS);
        }
    }

    protected void resetBackoff() {
        backoff.reset();
    }

    @VisibleForTesting
    public ClientCnx cnx() {
        return CLIENT_CNX_UPDATER.get(this);
    }

    protected void setClientCnx(ClientCnx clientCnx) {
        CLIENT_CNX_UPDATER.set(this, clientCnx);
    }

    private boolean isValidStateForReconnection() {
        State state = this.state.getState();
        switch (state) {
            case Uninitialized:
            case Connecting:
            case Ready:
                // Ok
                return true;

            case Closing:
            case Closed:
            case Failed:
            case Terminated:
                return false;
        }
        return false;
    }

    @VisibleForTesting
    public long getEpoch() {
        return epoch;
    }

    private static final Logger log = LoggerFactory.getLogger(ConnectionHandler.class);
}
