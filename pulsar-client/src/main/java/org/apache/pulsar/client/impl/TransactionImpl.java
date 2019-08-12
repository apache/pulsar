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
 *
 */

package org.apache.pulsar.client.impl;

import io.netty.buffer.ByteBuf;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Transaction;
import org.apache.pulsar.client.api.TxnId;
import org.apache.pulsar.client.impl.conf.TransactionConfigurationData;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.FutureUtil;

@Slf4j
public class TransactionImpl extends TransactionBase implements ConnectionHandler.Connection {

    private final ConnectionHandler connectionHandler;
    private TxnId txnId;
    private final long ttl;
    private final long createTxnIdTimeout;
    private final CompletableFuture<Transaction> transactionCreateFuture;

    enum Status {
        NEW,
        OPEN,
        COMMITTED,
        ABORTED,
    }

    private volatile Status txnStatus = Status.NEW;


    public TransactionImpl(PulsarClientImpl client, TransactionConfigurationData conf,
                           CompletableFuture<Transaction> transactionCreateFuture) {
        super(client, conf.getTopic());
        this.connectionHandler =
            new ConnectionHandler(this,
                                  new BackoffBuilder().setInitialTime(100, TimeUnit.MILLISECONDS)
                                                      .setMax(60, TimeUnit.SECONDS)
                                                      .setMandatoryStop(0, TimeUnit.MILLISECONDS)
                                                      .useUserConfiguredIntervals(
                                                          Backoff.DEFAULT_INTERVAL_IN_NANOSECONDS,
                                                          Backoff.MAX_BACKOFF_INTERVAL_NANOSECONDS)
                                                      .create(), this);
        this.ttl = conf.getTtl();
        this.createTxnIdTimeout = System.currentTimeMillis() + ttl;
        this.transactionCreateFuture = transactionCreateFuture;
        synchronized (txnStatus) {
            this.txnStatus = Status.OPEN;
        }
        grabCnx();
    }

    @Override
    public TxnId getTxnId() {
        return txnId;
    }

    @Override
    public boolean exist() {
        return false;
    }

    @Override
    public CompletableFuture<Void> asyncExist() {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public boolean isOpen() {
        return false;
    }

    @Override
    public CompletableFuture<Void> asyncIsOpen() {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public boolean isCommitting() {
        return false;
    }

    @Override
    public CompletableFuture<Void> asyncIsCommitting() {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public boolean isCommitted() {
        return false;
    }

    @Override
    public CompletableFuture<Void> asyncIsCommitted() {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public boolean isAborting() {
        return false;
    }

    @Override
    public CompletableFuture<Void> asyncIsAborting() {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public boolean isAborted() {
        return false;
    }

    @Override
    public CompletableFuture<Void> asyncIsAborted() {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public boolean commitTxn() {
        try {
            asyncCommitTxn().get();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    @Override
    public CompletableFuture<Void> asyncCommitTxn() {
        CompletableFuture<Void> commitFuture = new CompletableFuture<>();
        long requestId = client.newRequestId();
        ByteBuf commitTxn = Commands.newEndTxnOnPartition(requestId, txnId.getLeastBits(), txnId.getMostBits(), topic,
                                                    PulsarApi.TxnAction.COMMIT);
        cnx().sendTxnRequestWithId(commitTxn, requestId).whenComplete((response, err) -> {
            if (err != null) {
                commitFuture.completeExceptionally(err);
            } else {
                synchronized (txnStatus) {
                    this.txnStatus = Status.COMMITTED;
                }
                commitFuture.complete(null);
            }
        });

        return commitFuture;
    }

    @Override
    public boolean abortTxn() {
        try {
            asyncAbortTxn().get();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    @Override
    public CompletableFuture<Void> asyncAbortTxn() {
        CompletableFuture<Void> abortFuture = new CompletableFuture<>();
        long requestId = client.newRequestId();
        ByteBuf abortTxn = Commands.newEndTxnOnPartition(requestId, txnId.getLeastBits(), txnId.getMostBits(), topic,
                                                         PulsarApi.TxnAction.ABORT);
        cnx().sendTxnRequestWithId(abortTxn, requestId).whenComplete((response, err) -> {
            if (err != null) {
                abortFuture.completeExceptionally(err);
            } else {
                synchronized (txnStatus) {
                    this.txnStatus = Status.ABORTED;
                }
                abortFuture.complete(null);
            }
        });

        return abortFuture;
    }

    @Override
    String getHandlerName() {
        if (txnId != null) {
            return txnId.toString();
        } else {
            return new TxnIdImpl(0, 0).toString();
        }
    }

    @Override
    public void connectionFailed(PulsarClientException exception) {
        if (System.currentTimeMillis() > createTxnIdTimeout
            && transactionCreateFuture.completeExceptionally(exception)) {
            log.info("[{}] transaction create failed", topic);
            setState(State.Failed);
        }
    }

    @Override
    public void connectionOpened(final ClientCnx cnx) {
        setClientCnx(cnx);

        long requestId = client.newRequestId();
        cnx.sendTxnRequestWithId(Commands.newTxn(requestId, ttl), requestId).thenAccept(response -> {
            long txnIdMostBits = response.getTxnIdMostBiits();
            long txnIdLeastBits = response.getTxnIdLeastBits();
            if (this.txnId == null) {
                this.txnId = new TxnIdImpl(txnIdMostBits, txnIdLeastBits);
            }
            transactionCreateFuture.complete(TransactionImpl.this);
        }).exceptionally(err -> {
            setState(State.Failed);
            transactionCreateFuture.completeExceptionally(err);
            return null;
        });
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
