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
import io.netty.util.Recycler;
import io.netty.util.ReferenceCountUtil;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.TransactionMetaStoreClientException;
import org.apache.pulsar.client.impl.ClientCnx.RequestTime;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.pulsar.common.util.collections.GrowableArrayBlockingQueue;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Handler for transaction meta store.
 */
public class TransactionMetaStoreHandler extends HandlerState implements ConnectionHandler.Connection, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionMetaStoreHandler.class);

    private final long transactionCoordinatorId;
    private ConnectionHandler connectionHandler;
    private final ConcurrentLongHashMap<OpBase<?>> pendingRequests =
        new ConcurrentLongHashMap<>(16, 1);
    private final ConcurrentLinkedQueue<RequestTime> timeoutQueue;

    private final boolean blockIfReachMaxPendingOps;
    private final Semaphore semaphore;


    public TransactionMetaStoreHandler(long transactionCoordinatorId, PulsarClientImpl pulsarClient, String topic) {
        super(pulsarClient, topic);
        this.transactionCoordinatorId = transactionCoordinatorId;
        this.timeoutQueue = new ConcurrentLinkedQueue<>();
        this.blockIfReachMaxPendingOps = true;
        this.semaphore = new Semaphore(1000);
        this.connectionHandler = new ConnectionHandler(
            this,
            new BackoffBuilder()
                .setInitialTime(pulsarClient.getConfiguration().getInitialBackoffIntervalNanos(), TimeUnit.NANOSECONDS)
                .setMax(pulsarClient.getConfiguration().getMaxBackoffIntervalNanos(), TimeUnit.NANOSECONDS)
                .setMandatoryStop(100, TimeUnit.MILLISECONDS)
                .create(),
            this);
        this.connectionHandler.grabCnx();
    }

    @Override
    public void connectionFailed(PulsarClientException exception) {
        LOG.error("Transaction meta handler with transaction coordinator id {} connection failed.",
            transactionCoordinatorId, exception);
        setState(State.Failed);
    }

    @Override
    public void connectionOpened(ClientCnx cnx) {
        LOG.info("Transaction meta handler with transaction coordinator id {} connection opened.",
            transactionCoordinatorId);
        connectionHandler.setClientCnx(cnx);
        cnx.registerTransactionMetaStoreHandler(transactionCoordinatorId, this);
    }


    public CompletableFuture<TxnID> newTransactionAsync(long timeout, TimeUnit unit) {
        CompletableFuture<TxnID> callback = new CompletableFuture<>();

        if (!canSendRequest(callback)) {
            return callback;
        }
        long requestId = client.newRequestId();
        ByteBuf cmd = Commands.newTxn(requestId, unit.toMillis(timeout));
        OpNewTransaction op = OpNewTransaction.create(cmd, callback);
        pendingRequests.put(requestId, op);
        timeoutQueue.add(new RequestTime(System.currentTimeMillis(), requestId));
        cmd.retain();
        cnx().ctx().writeAndFlush(cmd, cnx().ctx().voidPromise());
        return callback;
    }

    void handleNewTxnResponse(PulsarApi.CommandNewTxnResponse response) {
        OpNewTransaction op = (OpNewTransaction) pendingRequests.remove(response.getRequestId());
        if (op == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Got new txn response for timeout {} - {}", response.getTxnidMostBits(),
                    response.getTxnidLeastBits());
            }
            return;
        }
        if (!response.hasError()) {
            op.callback.complete(new TxnID(response.getTxnidMostBits(),
                response.getTxnidLeastBits()));
        } else {
            PulsarClientException
        }

        ReferenceCountUtil.safeRelease(op.cmd);
        op.recycle();
        semaphore.release();
    }

    private static abstract class OpBase<T> {
        protected ByteBuf cmd;
        protected CompletableFuture<T> callback;
    }

    private static class OpNewTransaction extends OpBase<TxnID> {

        static OpNewTransaction create(ByteBuf cmd, CompletableFuture<TxnID> callback) {
            OpNewTransaction op = RECYCLER.get();
            op.callback = callback;
            op.cmd = cmd;
            return op;
        }

        private OpNewTransaction(Recycler.Handle<OpNewTransaction> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        void recycle() {
            recyclerHandle.recycle(this);
        }

        private final Recycler.Handle<OpNewTransaction> recyclerHandle;
        private static final Recycler<OpNewTransaction> RECYCLER = new Recycler<OpNewTransaction>() {
            @Override
            protected OpNewTransaction newObject(Handle<OpNewTransaction> handle) {
                return new OpNewTransaction(handle);
            }
        };
    }

    private boolean canSendRequest(CompletableFuture<?> callback) {
        try {
            if (blockIfReachMaxPendingOps) {
                semaphore.acquire();
            } else {
                if (!semaphore.tryAcquire()) {
                    callback.completeExceptionally(new TransactionMetaStoreClientException("Reach max pending ops."));
                    return false;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            callback.completeExceptionally(TransactionMetaStoreClientException.unwrap(e));
            return false;
        }
        return true;
    }

    private ClientCnx cnx() {
        return this.connectionHandler.cnx();
    }

    void connectionClosed(ClientCnx cnx) {
        this.connectionHandler.connectionClosed(cnx);
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public String getHandlerName() {
        return "Transaction meta store handler [" + transactionCoordinatorId + "]";
    }
}
