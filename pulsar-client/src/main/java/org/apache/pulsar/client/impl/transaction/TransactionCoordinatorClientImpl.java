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
package org.apache.pulsar.client.impl.transaction;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.BackoffBuilder;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConnectionHandler;
import org.apache.pulsar.client.impl.HandlerState;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * The implementation of {@link TransactionCoordinatorClient}.
 */
@Slf4j
public class TransactionCoordinatorClientImpl extends HandlerState implements TransactionCoordinatorClient, ConnectionHandler.Connection {

    private final PulsarClientImpl client;
    private final ConnectionHandler connectionHandler;

    private long clientConnectTimeout;

    public TransactionCoordinatorClientImpl(PulsarClientImpl client, String topic) {
        super(client, topic);
        this.client = client;
        this.connectionHandler = createConnectionHandler();
        this.clientConnectTimeout = client.getConfiguration().getOperationTimeoutMs() + System.currentTimeMillis();
        grabCnx();
    }

    @VisibleForTesting
    TransactionCoordinatorClientImpl(PulsarClientImpl client, String topic, ConnectionHandler connectionHandler) {
        super(client, topic);
        this.client = client;
        this.connectionHandler = connectionHandler;
        grabCnx();
    }

    ConnectionHandler createConnectionHandler() {
        return new ConnectionHandler(this,
                                     new BackoffBuilder().setInitialTime(100, TimeUnit.MILLISECONDS).setMax(60, TimeUnit.SECONDS)
                                                         .setMandatoryStop(0, TimeUnit.MILLISECONDS)
                                                         .useUserConfiguredIntervals(client.getConfiguration().getDefaultBackoffIntervalNanos(),
                                                                                     client.getConfiguration().getMaxBackoffIntervalNanos())
                                                         .create(), this);
    }

    @Override
    public CompletableFuture<Void> commitTxnOnTopic(String topic, long txnIdMostBits, long txnIdLeastBits) {
        long requestId = client.newRequestId();
        ByteBuf commitTxn = Commands.newEndTxnOnPartition(requestId, txnIdLeastBits, txnIdMostBits, topic,
                                                          PulsarApi.TxnAction.COMMIT);
        return sendRequest(commitTxn, requestId);
    }

    @Override
    public CompletableFuture<Void> abortTxnOnTopic(String topic, long txnIdMostBits, long txnIdLeastBits) {
        long requestId = client.newRequestId();
        ByteBuf abortTxn = Commands.newEndTxnOnPartition(requestId, txnIdLeastBits, txnIdMostBits, topic,
                                                         PulsarApi.TxnAction.ABORT);
        return sendRequest(abortTxn, requestId);
    }

    @Override
    public CompletableFuture<Void> commitTxnOnSubscription(String topic, String subscription, long txnIdMostBits, long txnIdLeastBits) {
        return FutureUtil.failedFuture(new UnsupportedOperationException("Not Implemented Yet"));
    }

    @Override
    public CompletableFuture<Void> abortTxnOnSubscription(String topic, String subscription, long txnIdMostBits, long txnIdLeastBits) {
        return FutureUtil.failedFuture(new UnsupportedOperationException("Not Implemented Yet"));
    }

    CompletableFuture<Void> sendRequest(ByteBuf msg, long requestId) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        cnx().sendTxnRequestWithId(msg, requestId).whenComplete((response, err) -> {
            if (err != null) {
                future.completeExceptionally(err);
                msg.release();
            } else {
                future.complete(null);
                msg.release();
            }
        });
        return future;
    }

    @Override
    public void connectionFailed(PulsarClientException exception) {
        if (System.currentTimeMillis() > clientConnectTimeout) {
            setState(State.Failed);
            log.warn("Transaction coordinator client connection timeout");
        }
    }

    @Override
    public void connectionOpened(ClientCnx cnx) {
        setClientCnx(cnx);
    }

    @Override
    protected String getHandlerName() {
        return "TransactionCoordinatorClient";
    }

    void setClientCnx(ClientCnx clientCnx) {
        this.connectionHandler.setClientCnx(clientCnx);
    }

    ClientCnx cnx() {
        return this.connectionHandler.cnx();
    }

    void grabCnx() {
        this.connectionHandler.grabCnx();
    }
}
