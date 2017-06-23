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
import java.util.function.UnaryOperator;

import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class HandlerBase {
    protected final PulsarClientImpl client;
    protected final String topic;
    private static final AtomicReferenceFieldUpdater<HandlerBase, State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(HandlerBase.class, State.class, "state");
    @SuppressWarnings("unused")
    private volatile State state = null;

    private static final AtomicReferenceFieldUpdater<HandlerBase, ClientCnx> CLIENT_CNX_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(HandlerBase.class, ClientCnx.class, "clientCnx");
    @SuppressWarnings("unused")
    private volatile ClientCnx clientCnx = null;
    protected final Backoff backoff;

    enum State {
        Uninitialized, // Not initialized
        Connecting, // Client connecting to broker
        Ready, // Handler is being used
        Closing, // Close cmd has been sent to broker
        Closed, // Broker acked the close
        Terminated, // Topic associated with this handler
                    // has been terminated
        Failed // Handler is failed
    };

    public HandlerBase(PulsarClientImpl client, String topic) {
        this.client = client;
        this.topic = topic;
        this.backoff = new Backoff(100, TimeUnit.MILLISECONDS, 60, TimeUnit.SECONDS);
        STATE_UPDATER.set(this, State.Uninitialized);
        CLIENT_CNX_UPDATER.set(this, null);
    }

    protected void grabCnx() {
        if (CLIENT_CNX_UPDATER.get(this) != null) {
            log.warn("[{}] [{}] Client cnx already set, ignoring reconnection request", topic, getHandlerName());
            return;
        }

        if (!isValidStateForReconnection()) {
            // Ignore connection closed when we are shutting down
            log.info("[{}] [{}] Ignoring reconnection request (state: {})", topic, getHandlerName(), STATE_UPDATER.get(this));
            return;
        }

        try {
            client.getConnection(topic) //
                    .thenAccept(this::connectionOpened) //
                    .exceptionally(this::handleConnectionError);
        } catch (Throwable t) {
            log.warn("[{}] [{}] Exception thrown while getting connection: ", topic, getHandlerName(), t);
            reconnectLater(t);
        }
    }

    private Void handleConnectionError(Throwable exception) {
        log.warn("[{}] [{}] Error connecting to broker: {}", topic, getHandlerName(), exception.getMessage());
        connectionFailed(new PulsarClientException(exception));

        State state = STATE_UPDATER.get(this);
        if (state == State.Uninitialized || state == State.Connecting || state == State.Ready) {
            reconnectLater(exception);
        }

        return null;
    }

    protected void reconnectLater(Throwable exception) {
        CLIENT_CNX_UPDATER.set(this, null);
        if (!isValidStateForReconnection()) {
            log.info("[{}] [{}] Ignoring reconnection request (state: {})", topic, getHandlerName(), STATE_UPDATER.get(this));
            return;
        }
        long delayMs = backoff.next();
        log.warn("[{}] [{}] Could not get connection to broker: {} -- Will try again in {} s", topic, getHandlerName(),
                exception.getMessage(), delayMs / 1000.0);
        STATE_UPDATER.set(this, State.Connecting);
        client.timer().newTimeout(timeout -> {
            log.info("[{}] [{}] Reconnecting after connection was closed", topic, getHandlerName());
            grabCnx();
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    protected void connectionClosed(ClientCnx cnx) {
        if (CLIENT_CNX_UPDATER.compareAndSet(this, cnx, null)) {
            if (!isValidStateForReconnection()) {
                log.info("[{}] [{}] Ignoring reconnection request (state: {})", topic, getHandlerName(), STATE_UPDATER.get(this));
                return;
            }
            long delayMs = backoff.next();
            STATE_UPDATER.set(this, State.Connecting);
            log.info("[{}] [{}] Closed connection {} -- Will try again in {} s", topic, getHandlerName(), cnx.channel(),
                    delayMs / 1000.0);
            client.timer().newTimeout(timeout -> {
                log.warn("[{}] [{}] Reconnecting after timeout", topic, getHandlerName());
                grabCnx();
            }, delayMs, TimeUnit.MILLISECONDS);
        }
    }

    protected void resetBackoff() {
        backoff.reset();
    }

    protected ClientCnx cnx() {
        return CLIENT_CNX_UPDATER.get(this);
    }

    protected boolean isRetriableError(PulsarClientException e) {
        return e instanceof PulsarClientException.LookupException;
    }

    // moves the state to ready if it wasn't closed
    protected boolean changeToReadyState() {
        return (STATE_UPDATER.compareAndSet(this, State.Uninitialized, State.Ready)
                || STATE_UPDATER.compareAndSet(this, State.Connecting, State.Ready));
    }

    protected State getState() {
        return STATE_UPDATER.get(this);
    }

    protected void setState(State s) {
        STATE_UPDATER.set(this, s);
    }

    protected State getAndUpdateState(final UnaryOperator<State> updater) {
        return STATE_UPDATER.getAndUpdate(this, updater);
    }

    protected ClientCnx getClientCnx() {
        return CLIENT_CNX_UPDATER.get(this);
    }

    protected void setClientCnx(ClientCnx clientCnx) {
        CLIENT_CNX_UPDATER.set(this, clientCnx);
    }

    private boolean isValidStateForReconnection() {
        State state = STATE_UPDATER.get(this);
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

    abstract void connectionFailed(PulsarClientException exception);

    abstract void connectionOpened(ClientCnx cnx);

    abstract String getHandlerName();

    private static final Logger log = LoggerFactory.getLogger(HandlerBase.class);
}
