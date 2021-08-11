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

import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.CursorClient;
import org.apache.pulsar.client.api.CursorData;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.cursor.CursorDataImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.FutureUtil;

@Slf4j
public class CursorClientImpl extends HandlerState implements CursorClient, ConnectionHandler.Connection {

    private final PulsarClientImpl client;
    private final String topic;
    private final ConnectionHandler connectionHandler;
    private final long clientId;

    private final List<Throwable> previousExceptions = new CopyOnWriteArrayList<Throwable>();
    private final CompletableFuture<CursorClient> cursorClientFuture;
    private final long lookupDeadline;

    public CursorClientImpl(PulsarClientImpl pulsarClient, String topic,
                            CompletableFuture<CursorClient> cursorClientFuture) {
        super(pulsarClient, topic);
        this.client = pulsarClient;
        this.clientId = client.newCursorClientId();
        this.topic = topic;
        this.connectionHandler = new ConnectionHandler(this,
                new BackoffBuilder()
                        .setInitialTime(client.getConfiguration().getInitialBackoffIntervalNanos(),
                                TimeUnit.NANOSECONDS)
                        .setMax(client.getConfiguration().getMaxBackoffIntervalNanos(), TimeUnit.NANOSECONDS)
                        .setMandatoryStop(0, TimeUnit.MILLISECONDS)
                        .create(),
                this);
        this.cursorClientFuture = cursorClientFuture;
        this.lookupDeadline = System.currentTimeMillis() + client.getConfiguration().getLookupTimeoutMs();
        grabCnx();
    }

    void grabCnx() {
        this.connectionHandler.grabCnx();
    }

    @Override
    String getHandlerName() {
        return "CursorClient[" + topic + "]#" + clientId;
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public CompletableFuture<CursorData> getCursorAsync(String subscription) {
        if (getState() == State.Closing || getState() == State.Closed) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException(
                    String.format("The cursor client %s was already closed when the subscription %s of the topic %s " +
                            "getting cursor data", clientId, subscription, topic)));
        }

        ClientCnx cnx = this.connectionHandler.cnx();
        if (!Commands.peerSupportsCursorOps(cnx.getRemoteEndpointProtocolVersion())) {
            return FutureUtil.failedFuture(new PulsarClientException.NotSupportedException(
                    String.format("The command `GetCursor` is not supported for the protocol version %d. " +
                                    "The cursor client is %s, topic %s, subscription %s",
                            cnx.getRemoteEndpointProtocolVersion(), clientId, topic, subscription)));
        }
        CompletableFuture<CursorData> future = new CompletableFuture<>();
        long requestId = client.newRequestId();
        ByteBuf cmdBytes = Commands.newGetCursor(requestId, topic, subscription);
        log.info("[{}][{}] Get Cursor", topic, subscription);
        cnx.sendGetCursor(cmdBytes, requestId).thenAccept(cmd -> {
            future.complete(new CursorDataImpl(
                    cmd.getManagedLedgerVersion(),
                    cmd.getLastConfirmedEntry(),
                    cmd.getPosition())
            );
        }).exceptionally(e -> {
            log.error("[{}][{}] Failed GetCursor command", topic, subscription);
            future.completeExceptionally(PulsarClientException.wrap(e.getCause(),
                    String.format("The subscription %s of the topic %s get cursor info was failed",
                            subscription, topic)));
            return null;
        });
        return future;
    }


    @Override
    public CompletableFuture<CursorData> createCursorAsync(String subscription, CursorData cursorData) {
        if (getState() == State.Closing || getState() == State.Closed) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException(
                    String.format("The cursor client %s was already closed when the subscription %s of the topic %s " +
                            "creating cursor", clientId, subscription, topic)));
        }

        ClientCnx cnx = this.connectionHandler.cnx();
        if (!Commands.peerSupportsCursorOps(cnx.getRemoteEndpointProtocolVersion())) {
            return FutureUtil.failedFuture(new PulsarClientException.NotSupportedException(
                    String.format("The command `CreateCursor` is not supported for the protocol version %d. " +
                                    "The cursor client is %s, topic %s, subscription %s",
                            cnx.getRemoteEndpointProtocolVersion(), clientId, topic, subscription)));
        }
        CompletableFuture<CursorData> future = new CompletableFuture<>();
        long requestId = client.newRequestId();
        ByteBuf cmdBytes = Commands.newCreateCursor(requestId, topic, subscription,
                (CursorDataImpl) cursorData);
        log.info("[{}][{}] Create Cursor", topic, subscription);
        cnx.sendCreateCursor(cmdBytes, requestId).thenAccept(cmd -> {
            future.complete(new CursorDataImpl(
                    cmd.getManagedLedgerVersion(),
                    cmd.getLastConfirmedEntry(),
                    cmd.getPosition())
            );
        }).exceptionally(e -> {
            log.error("[{}][{}] Failed CreateCursor command", topic, subscription);
            future.completeExceptionally(PulsarClientException.wrap(e.getCause(),
                    String.format("The subscription %s of the topic %s create cursor was failed",
                            subscription, topic)));
            return null;
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> deleteCursorAsync(String subscription) {
        if (getState() == State.Closing || getState() == State.Closed) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException(
                    String.format("The cursor client %s was already closed when the subscription %s of the topic %s " +
                            "deleting cursor", clientId, subscription, topic)));
        }

        ClientCnx cnx = this.connectionHandler.cnx();
        if (!Commands.peerSupportsCursorOps(cnx.getRemoteEndpointProtocolVersion())) {
            return FutureUtil.failedFuture(new PulsarClientException.NotSupportedException(
                    String.format("The command `DeleteCursor` is not supported for the protocol version %d. " +
                                    "The cursor client is %s, topic %s, subscription %s",
                            cnx.getRemoteEndpointProtocolVersion(), clientId, topic, subscription)));
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        long requestId = client.newRequestId();
        ByteBuf cmdBytes = Commands.newDeleteCursor(requestId, topic, subscription);
        log.info("[{}][{}] Delete Cursor", topic, subscription);
        cnx.sendDeleteCursor(cmdBytes, requestId).thenAccept(v -> {
            future.complete(null);
        }).exceptionally(e -> {
            log.error("[{}][{}] Failed DeleteCursor command", topic, subscription);
            future.completeExceptionally(PulsarClientException.wrap(e.getCause(),
                    String.format("The subscription %s of the topic %s delete cursor was failed",
                            subscription, topic)));
            return null;
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> updateCursorAsync(String subscription, CursorData cursorData) {
        if (getState() == State.Closing || getState() == State.Closed) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException(
                    String.format("The cursor client %s was already closed when the subscription %s of the topic %s " +
                            "updating cursor", clientId, subscription, topic)));
        }

        ClientCnx cnx = this.connectionHandler.cnx();
        if (!Commands.peerSupportsCursorOps(cnx.getRemoteEndpointProtocolVersion())) {
            return FutureUtil.failedFuture(new PulsarClientException.NotSupportedException(
                    String.format("The command `UpdateCursor` is not supported for the protocol version %d. " +
                                    "The cursor client is %s, topic %s, subscription %s",
                            cnx.getRemoteEndpointProtocolVersion(), clientId, topic, subscription)));
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        long requestId = client.newRequestId();
        ByteBuf cmdBytes =
                Commands.newUpdateCursor(requestId, topic, subscription, (CursorDataImpl) cursorData);
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Update Cursor", topic, subscription);
        }
        cnx.sendUpdateCursor(cmdBytes, requestId).thenAccept(v -> {
            future.complete(null);
        }).exceptionally(e -> {
            log.error("[{}][{}] Failed UpdateCursor command", topic, subscription);
            future.completeExceptionally(PulsarClientException.wrap(e.getCause(),
                    String.format("The subscription %s of the topic %s update cursor was failed",
                            subscription, topic)));
            return null;
        });
        return future;
    }

    @Override
    public void close() throws PulsarClientException {
        try {
            closeAsync().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw PulsarClientException.unwrap(e);
        } catch (ExecutionException e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        final State currentState = getAndUpdateState(state -> {
            if (state == State.Closed) {
                return state;
            }
            return State.Closing;
        });

        if (currentState == State.Closed || currentState == State.Closing) {
            closeFuture.complete(null);
            return closeFuture;
        }
        log.info("[{}] [{}] Closed CursorClient", topic, clientId);

        setState(State.Closed);
        client.cleanupCursorClient(this);

        ClientCnx cnx = this.connectionHandler.cnx();
        if (cnx != null) {
            cnx.removeCursorClient(clientId);
        }
        closeFuture.complete(null);
        return closeFuture;
    }

    void connectionClosed(ClientCnx cnx) {
        this.connectionHandler.connectionClosed(cnx);
    }

    @Override
    public void connectionFailed(PulsarClientException exception) {
        log.error("CursorClientImpl for topic {} with id {} connection failed.", topic, clientId);

        boolean nonRetriableError = !PulsarClientException.isRetriableError(exception);
        boolean timeout = System.currentTimeMillis() > lookupDeadline;
        if (nonRetriableError || timeout) {
            exception.setPreviousExceptions(previousExceptions);
            if (cursorClientFuture.completeExceptionally(exception)) {
                if (nonRetriableError) {
                    log.info("[{}] CursorClient creation failed for clientId {} with unretriableError = {}",
                            topic, clientId, exception);
                } else {
                    log.info("[{}] CursorClient creation failed for client {} after timeout", topic, clientId);
                }
                setState(State.Failed);
                client.cleanupCursorClient(this);
            }
        } else {
            previousExceptions.add(exception);
        }
    }

    @Override
    public void connectionOpened(ClientCnx cnx) {
        log.info("CursorClientImpl for topic {} with id {} connection opened on cnx {}", topic, clientId,
                cnx.ctx().channel());

        previousExceptions.clear();

        connectionHandler.setClientCnx(cnx);
        cnx.registerCursorClient(clientId, this);
        if (changeToReadyState()) {
            cursorClientFuture.complete(this);
        } else {
            cnx.channel().close();
        }
    }
}
