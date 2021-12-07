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

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandAddPartitionToTxnResponse;
import org.apache.pulsar.common.api.proto.CommandAddSubscriptionToTxnResponse;
import org.apache.pulsar.common.api.proto.CommandEndTxnResponse;
import org.apache.pulsar.common.api.proto.CommandNewTxnResponse;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.api.proto.Subscription;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Handler for transaction meta store.
 */
public class TransactionMetaStoreHandler extends HandlerState implements ConnectionHandler.Connection, Closeable, TimerTask {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionMetaStoreHandler.class);

    private final long transactionCoordinatorId;
    private final ConnectionHandler connectionHandler;
    private final ConcurrentLongHashMap<OpBase<?>> pendingRequests =
        new ConcurrentLongHashMap<>(16, 1);
    private final ConcurrentLinkedQueue<RequestTime> timeoutQueue;

    private static class RequestTime {
        final long creationTimeMs;
        final long requestId;

        public RequestTime(long creationTime, long requestId) {
            this.creationTimeMs = creationTime;
            this.requestId = requestId;
        }
    }

    private final boolean blockIfReachMaxPendingOps;
    private final Semaphore semaphore;

    private Timeout requestTimeout;

    private final CompletableFuture<Void> connectFuture;

    public TransactionMetaStoreHandler(long transactionCoordinatorId, PulsarClientImpl pulsarClient, String topic,
                                       CompletableFuture<Void> connectFuture) {
        super(pulsarClient, topic);
        this.transactionCoordinatorId = transactionCoordinatorId;
        this.timeoutQueue = new ConcurrentLinkedQueue<>();
        this.blockIfReachMaxPendingOps = true;
        this.semaphore = new Semaphore(1000);
        this.requestTimeout = pulsarClient.timer().newTimeout(this, pulsarClient.getConfiguration().getOperationTimeoutMs(), TimeUnit.MILLISECONDS);
        this.connectionHandler = new ConnectionHandler(
            this,
            new BackoffBuilder()
                .setInitialTime(pulsarClient.getConfiguration().getInitialBackoffIntervalNanos(), TimeUnit.NANOSECONDS)
                .setMax(pulsarClient.getConfiguration().getMaxBackoffIntervalNanos(), TimeUnit.NANOSECONDS)
                .setMandatoryStop(100, TimeUnit.MILLISECONDS)
                .create(),
            this);
        this.connectFuture = connectFuture;
        this.connectionHandler.grabCnx();
    }

    @Override
    public void connectionFailed(PulsarClientException exception) {
        LOG.error("Transaction meta handler with transaction coordinator id {} connection failed.",
            transactionCoordinatorId, exception);
        if (!this.connectFuture.isDone()) {
            this.connectFuture.completeExceptionally(exception);
        }
    }

    @Override
    public void connectionOpened(ClientCnx cnx) {
        LOG.info("Transaction meta handler with transaction coordinator id {} connection opened.",
            transactionCoordinatorId);

        if (getState() == State.Closing || getState() == State.Closed) {
            setState(State.Closed);
            failPendingRequest();
            this.pendingRequests.clear();
            return;
        }

        connectionHandler.setClientCnx(cnx);
        cnx.registerTransactionMetaStoreHandler(transactionCoordinatorId, this);

        // if broker protocol version < 19, don't send TcClientConnectRequest to broker.
        if (cnx.getRemoteEndpointProtocolVersion() > ProtocolVersion.v18.getValue()) {
            long requestId = client.newRequestId();
            ByteBuf request = Commands.newTcClientConnectRequest(transactionCoordinatorId, requestId);

            cnx.sendRequestWithId(request, requestId).thenRun(() -> {
                LOG.info("Transaction coordinator client connect success! tcId : {}", transactionCoordinatorId);
                if (!changeToReadyState()) {
                    setState(State.Closed);
                    cnx.channel().close();
                }

                if (!this.connectFuture.isDone()) {
                    this.connectFuture.complete(null);
                }
                this.connectionHandler.resetBackoff();
            }).exceptionally((e) -> {
                LOG.error("Transaction coordinator client connect fail! tcId : {}",
                        transactionCoordinatorId, e.getCause());
                if (getState() == State.Closing || getState() == State.Closed
                        || e.getCause() instanceof PulsarClientException.NotAllowedException) {
                    setState(State.Closed);
                    cnx.channel().close();
                } else {
                    connectionHandler.reconnectLater(e.getCause());
                }
                return null;
            });
        } else {
            if (!changeToReadyState()) {
                cnx.channel().close();
            }
            this.connectFuture.complete(null);
        }
    }

    private void failPendingRequest() {
        pendingRequests.keys().forEach(k -> {
            OpBase<?> op = pendingRequests.remove(k);
            if (op != null && !op.callback.isDone()) {
                op.callback.completeExceptionally(new PulsarClientException.AlreadyClosedException(
                        "Could not get response from transaction meta store when " +
                                "the transaction meta store has already close."));
                onResponse(op);
            }
        });
    }

    public CompletableFuture<TxnID> newTransactionAsync(long timeout, TimeUnit unit) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("New transaction with timeout in ms {}", unit.toMillis(timeout));
        }
        CompletableFuture<TxnID> callback = new CompletableFuture<>();

        if (!canSendRequest(callback)) {
            return callback;
        }
        long requestId = client.newRequestId();
        ByteBuf cmd = Commands.newTxn(transactionCoordinatorId, requestId, unit.toMillis(timeout));
        OpForTxnIdCallBack op = OpForTxnIdCallBack.create(cmd, callback);
        pendingRequests.put(requestId, op);
        timeoutQueue.add(new RequestTime(System.currentTimeMillis(), requestId));
        cmd.retain();
        cnx().ctx().writeAndFlush(cmd, cnx().ctx().voidPromise());
        return callback;
    }

    void handleNewTxnResponse(CommandNewTxnResponse response) {
        OpForTxnIdCallBack op = (OpForTxnIdCallBack) pendingRequests.remove(response.getRequestId());
        if (op == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Got new txn response for timeout {} - {}", response.getTxnidMostBits(),
                    response.getTxnidLeastBits());
            }
            return;
        }

        if (!response.hasError()) {
            TxnID txnID = new TxnID(response.getTxnidMostBits(), response.getTxnidLeastBits());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Got new txn response {} for request {}", txnID, response.getRequestId());
            }
            op.callback.complete(txnID);
        } else {
            LOG.error("Got new txn for request {} error {}", response.getRequestId(), response.getError());
            handleTransactionFailOp(response.getError(), response.getMessage(), op);
        }

        onResponse(op);
    }

    public CompletableFuture<Void> addPublishPartitionToTxnAsync(TxnID txnID, List<String> partitions) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Add publish partition {} to txn {}", partitions, txnID);
        }
        CompletableFuture<Void> callback = new CompletableFuture<>();

        if (!canSendRequest(callback)) {
            return callback;
        }
        long requestId = client.newRequestId();
        ByteBuf cmd = Commands.newAddPartitionToTxn(
                requestId, txnID.getLeastSigBits(), txnID.getMostSigBits(), partitions);
        OpForVoidCallBack op = OpForVoidCallBack.create(cmd, callback);
        pendingRequests.put(requestId, op);
        timeoutQueue.add(new RequestTime(System.currentTimeMillis(), requestId));
        cmd.retain();
        cnx().ctx().writeAndFlush(cmd, cnx().ctx().voidPromise());
        return callback;
    }

    void handleAddPublishPartitionToTxnResponse(CommandAddPartitionToTxnResponse response) {
        OpForVoidCallBack op = (OpForVoidCallBack) pendingRequests.remove(response.getRequestId());
        if (op == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Got add publish partition to txn response for timeout {} - {}", response.getTxnidMostBits(),
                        response.getTxnidLeastBits());
            }
            return;
        }

        if (!response.hasError()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Add publish partition for request {} success.", response.getRequestId());
            }
            op.callback.complete(null);
        } else {
            LOG.error("Add publish partition for request {} error {}.", response.getRequestId(), response.getError());
            handleTransactionFailOp(response.getError(), response.getMessage(), op);
        }

        onResponse(op);
    }

    public CompletableFuture<Void> addSubscriptionToTxn(TxnID txnID, List<Subscription> subscriptionList) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Add subscription {} to txn {}.", subscriptionList, txnID);
        }

        CompletableFuture<Void> callback = new CompletableFuture<>();

        if (!canSendRequest(callback)) {
            return callback;
        }
        long requestId = client.newRequestId();
        ByteBuf cmd = Commands.newAddSubscriptionToTxn(
                requestId, txnID.getLeastSigBits(), txnID.getMostSigBits(), subscriptionList);
        OpForVoidCallBack op = OpForVoidCallBack.create(cmd, callback);
        pendingRequests.put(requestId, op);
        timeoutQueue.add(new RequestTime(System.currentTimeMillis(), requestId));
        cmd.retain();
        cnx().ctx().writeAndFlush(cmd, cnx().ctx().voidPromise());
        return callback;
    }

    public void handleAddSubscriptionToTxnResponse(CommandAddSubscriptionToTxnResponse response) {
        OpForVoidCallBack op = (OpForVoidCallBack) pendingRequests.remove(response.getRequestId());
        if (op == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Add subscription to txn timeout for request {}.", response.getRequestId());
            }
            return;
        }

        if (!response.hasError()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Add subscription to txn success for request {}.", response.getRequestId());
            }
            op.callback.complete(null);
        } else {
            LOG.error("Add subscription to txn failed for request {} error {}.",
                    response.getRequestId(), response.getError());
            handleTransactionFailOp(response.getError(), response.getMessage(), op);
        }
        onResponse(op);
    }

    public CompletableFuture<Void> endTxnAsync(TxnID txnID, TxnAction action) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("End txn {}, action {}", txnID, action);
        }
        CompletableFuture<Void> callback = new CompletableFuture<>();

        if (!canSendRequest(callback)) {
            return callback;
        }
        long requestId = client.newRequestId();
        BaseCommand cmd = Commands.newEndTxn(requestId, txnID.getLeastSigBits(), txnID.getMostSigBits(), action);
        ByteBuf buf = Commands.serializeWithSize(cmd);
        OpForVoidCallBack op = OpForVoidCallBack.create(buf, callback);
        pendingRequests.put(requestId, op);
        timeoutQueue.add(new RequestTime(System.currentTimeMillis(), requestId));
        buf.retain();
        cnx().ctx().writeAndFlush(buf, cnx().ctx().voidPromise());
        return callback;
    }

    void handleEndTxnResponse(CommandEndTxnResponse response) {
        OpForVoidCallBack op = (OpForVoidCallBack) pendingRequests.remove(response.getRequestId());
        if (op == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Got end txn response for timeout {} - {}", response.getTxnidMostBits(),
                        response.getTxnidLeastBits());
            }
            return;
        }

        if (!response.hasError()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Got end txn response success for request {}", response.getRequestId());
            }
            op.callback.complete(null);
        } else {
            LOG.error("Got end txn response for request {} error {}", response.getRequestId(), response.getError());
            handleTransactionFailOp(response.getError(), response.getMessage(), op);
        }

        onResponse(op);
    }

    private void handleTransactionFailOp(ServerError error, String message, OpBase<?> op) {
        if (error == ServerError.TransactionCoordinatorNotFound && getState() != State.Connecting) {
            connectionHandler.reconnectLater(new TransactionCoordinatorClientException
                    .CoordinatorNotFoundException(message));
        }

        if (op != null) {
            op.callback.completeExceptionally(getExceptionByServerError(error, message));
        }
    }

    private static abstract class OpBase<T> {
        protected ByteBuf cmd;
        protected CompletableFuture<T> callback;

        abstract void recycle();
    }

    private static class OpForTxnIdCallBack extends OpBase<TxnID> {

        static OpForTxnIdCallBack create(ByteBuf cmd, CompletableFuture<TxnID> callback) {
            OpForTxnIdCallBack op = RECYCLER.get();
            op.callback = callback;
            op.cmd = cmd;
            return op;
        }

        private OpForTxnIdCallBack(Recycler.Handle<OpForTxnIdCallBack> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        @Override
        void recycle() {
            recyclerHandle.recycle(this);
        }

        private final Recycler.Handle<OpForTxnIdCallBack> recyclerHandle;
        private static final Recycler<OpForTxnIdCallBack> RECYCLER = new Recycler<OpForTxnIdCallBack>() {
            @Override
            protected OpForTxnIdCallBack newObject(Handle<OpForTxnIdCallBack> handle) {
                return new OpForTxnIdCallBack(handle);
            }
        };
    }

    private static class OpForVoidCallBack extends OpBase<Void> {

        static OpForVoidCallBack create(ByteBuf cmd, CompletableFuture<Void> callback) {
            OpForVoidCallBack op = RECYCLER.get();
            op.callback = callback;
            op.cmd = cmd;
            return op;
        }
        private OpForVoidCallBack(Recycler.Handle<OpForVoidCallBack> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        @Override
        void recycle() {
            recyclerHandle.recycle(this);
        }

        private final Recycler.Handle<OpForVoidCallBack> recyclerHandle;
        private static final Recycler<OpForVoidCallBack> RECYCLER = new Recycler<OpForVoidCallBack>() {
            @Override
            protected OpForVoidCallBack newObject(Handle<OpForVoidCallBack> handle) {
                return new OpForVoidCallBack(handle);
            }
        };
    }

    public static TransactionCoordinatorClientException getExceptionByServerError(ServerError serverError, String msg) {
        switch (serverError) {
            case TransactionCoordinatorNotFound:
                return new TransactionCoordinatorClientException.CoordinatorNotFoundException(msg);
            case InvalidTxnStatus:
                return new TransactionCoordinatorClientException.InvalidTxnStatusException(msg);
            case TransactionNotFound:
                return new TransactionCoordinatorClientException.TransactionNotFoundException(msg);
            default:
                return new TransactionCoordinatorClientException(msg);
        }
    }

    private void onResponse(OpBase<?> op) {
        ReferenceCountUtil.safeRelease(op.cmd);
        op.recycle();
        semaphore.release();
    }

    private boolean canSendRequest(CompletableFuture<?> callback) {
        if (!isValidHandlerState(callback)) {
            return false;
        }
        try {
            if (blockIfReachMaxPendingOps) {
                semaphore.acquire();
            } else {
                if (!semaphore.tryAcquire()) {
                    callback.completeExceptionally(new TransactionCoordinatorClientException("Reach max pending ops."));
                    return false;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            callback.completeExceptionally(TransactionCoordinatorClientException.unwrap(e));
            return false;
        }
        return true;
    }

    private boolean isValidHandlerState(CompletableFuture<?> callback) {
        switch (getState()) {
            case Ready:
                return true;
            case Connecting:
                callback.completeExceptionally(
                        new TransactionCoordinatorClientException.MetaStoreHandlerNotReadyException(
                                "Transaction meta store handler for tcId "
                                + transactionCoordinatorId
                                + " is connecting now, please try later."));
                return false;
            case Closing:
            case Closed:
                callback.completeExceptionally(
                        new TransactionCoordinatorClientException.MetaStoreHandlerNotReadyException(
                                "Transaction meta store handler for tcId "
                                        + transactionCoordinatorId
                                        + " is closing or closed."));
                return false;
            case Failed:
            case Uninitialized:
                callback.completeExceptionally(
                        new TransactionCoordinatorClientException.MetaStoreHandlerNotReadyException(
                                "Transaction meta store handler for tcId "
                                        + transactionCoordinatorId
                                        + " not connected."));
                return false;
            default:
                callback.completeExceptionally(
                        new TransactionCoordinatorClientException.MetaStoreHandlerNotReadyException(
                                transactionCoordinatorId));
                return false;
        }
    }

    @Override
    public void run(Timeout timeout) throws Exception {
        if (timeout.isCancelled()) {
            return;
        }
        long timeToWaitMs;
        if (getState() == State.Closing || getState() == State.Closed) {
            return;
        }
        RequestTime peeked = timeoutQueue.peek();
        while (peeked != null && peeked.creationTimeMs + client.getConfiguration().getOperationTimeoutMs()
                - System.currentTimeMillis() <= 0) {
            RequestTime lastPolled = timeoutQueue.poll();
            if (lastPolled != null) {
                OpBase<?> op = pendingRequests.remove(lastPolled.requestId);
                if (op != null && !op.callback.isDone()) {
                    op.callback.completeExceptionally(new PulsarClientException.TimeoutException(
                            "Could not get response from transaction meta store within given timeout."));
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Transaction coordinator request {} is timeout.", lastPolled.requestId);
                    }
                    onResponse(op);
                }
            } else {
                break;
            }
            peeked = timeoutQueue.peek();
        }

        if (peeked == null) {
            timeToWaitMs = client.getConfiguration().getOperationTimeoutMs();
        } else {
            long diff = (peeked.creationTimeMs + client.getConfiguration().getOperationTimeoutMs()) - System.currentTimeMillis();
            if (diff <= 0) {
                timeToWaitMs = client.getConfiguration().getOperationTimeoutMs();
            } else {
                timeToWaitMs = diff;
            }
        }
        requestTimeout = client.timer().newTimeout(this, timeToWaitMs, TimeUnit.MILLISECONDS);
    }

    private ClientCnx cnx() {
        return this.connectionHandler.cnx();
    }

    void connectionClosed(ClientCnx cnx) {
        this.connectionHandler.connectionClosed(cnx);
    }

    @Override
    public void close() throws IOException {
        this.requestTimeout.cancel();
        this.setState(State.Closed);
    }

    @VisibleForTesting
    public State getConnectHandleState() {
        return getState();
    }

    @Override
    public String getHandlerName() {
        return "Transaction meta store handler [" + transactionCoordinatorId + "]";
    }
}
