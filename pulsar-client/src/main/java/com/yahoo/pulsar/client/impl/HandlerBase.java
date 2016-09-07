/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.client.impl;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.client.api.PulsarClientException;

abstract class HandlerBase {
    protected final PulsarClientImpl client;
    protected final String topic;
    protected AtomicReference<State> state = new AtomicReference<>();

    protected final AtomicReference<ClientCnx> clientCnx;
    protected final Backoff backoff;

    enum State {
        Uninitialized, // Not initialized
        Connecting, // Client connecting to broker
        Ready, // Handler is being used
        Closing, // Close cmd has been sent to broker
        Closed, // Broker acked the close
        Failed // Handler is failed
    };

    public HandlerBase(PulsarClientImpl client, String topic) {
        this.client = client;
        this.topic = topic;
        this.clientCnx = new AtomicReference<>();
        this.backoff = new Backoff(100, TimeUnit.MILLISECONDS, 60, TimeUnit.SECONDS);
        this.state.set(State.Uninitialized);
    }

    protected void grabCnx() {
        if (clientCnx.get() != null) {
            log.warn("[{}] [{}] Client cnx already set, ignoring reconnection request", topic, getHandlerName());
            return;
        }

        if (!isValidStateForReconnection()) {
            // Ignore connection closed when we are shutting down
            log.info("[{}] [{}] Ignoring reconnection request (state: {})", topic, getHandlerName(), state);
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

        State state = this.state.get();
        if (state == State.Uninitialized || state == State.Connecting || state == State.Ready) {
            reconnectLater(exception);
        }

        return null;
    }

    protected void reconnectLater(Throwable exception) {
        clientCnx.set(null);
        if (!isValidStateForReconnection()) {
            log.info("[{}] [{}] Ignoring reconnection request (state: {})", topic, getHandlerName(), state);
            return;
        }
        long delayMs = backoff.next();
        log.warn("[{}] [{}] Could not get connection to broker: {} -- Will try again in {} s", topic, getHandlerName(),
                exception.getMessage(), delayMs / 1000.0);
        state.set(State.Connecting);
        client.timer().newTimeout(timeout -> {
            log.info("[{}] [{}] Reconnecting after connection was closed", topic, getHandlerName());
            grabCnx();
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    protected void connectionClosed(ClientCnx cnx) {
        if (clientCnx.compareAndSet(cnx, null)) {
            if (!isValidStateForReconnection()) {
                log.info("[{}] [{}] Ignoring reconnection request (state: {})", topic, getHandlerName(), state);
                return;
            }
            long delayMs = backoff.next();
            state.set(State.Connecting);
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
        return clientCnx.get();
    }

    protected boolean isRetriableError(PulsarClientException e) {
        return e instanceof PulsarClientException.LookupException;
    }

    // moves the state to ready if it wasn't closed
    protected boolean changeToReadyState() {
        return (state.compareAndSet(State.Uninitialized, State.Ready)
                || state.compareAndSet(State.Connecting, State.Ready));
    }

    private boolean isValidStateForReconnection() {
        State state = this.state.get();
        switch (state) {
        case Uninitialized:
        case Connecting:
        case Ready:
            // Ok
            return true;
        case Closing:
        case Closed:
        case Failed:
            return false;
        }
        return false;
    }

    abstract void connectionFailed(PulsarClientException exception);

    abstract void connectionOpened(ClientCnx cnx);

    abstract String getHandlerName();

    private static final Logger log = LoggerFactory.getLogger(HandlerBase.class);
}
