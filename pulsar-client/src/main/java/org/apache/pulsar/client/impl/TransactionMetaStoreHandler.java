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
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
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

/**
 * Handler for transaction meta store.
 */
public class TransactionMetaStoreHandler extends HandlerState
        implements ConnectionHandler.Connection, Closeable, TimerTask {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionMetaStoreHandler.class);

    private final long transactionCoordinatorId;
    private final ConnectionHandler connectionHandler;
    private final ConcurrentLongHashMap<OpBase<?>> pendingRequests =
            ConcurrentLongHashMap.<OpBase<?>>newBuilder()
                    .expectedItems(16)
                    .concurrencyLevel(1)
                    .build();
    private final ConcurrentLinkedQueue<RequestTime> timeoutQueue;

    protected final Timer timer;
    private final ExecutorService internalPinnedExecutor;

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
        this.requestTimeout = pulsarClient.timer().newTimeout(this,
                pulsarClient.getConfiguration().getOperationTimeoutMs(), TimeUnit.MILLISECONDS);
        this.connectionHandler = new ConnectionHandler(
            this,
            new BackoffBuilder()
                .setInitialTime(pulsarClient.getConfiguration().getInitialBackoffIntervalNanos(), TimeUnit.NANOSECONDS)
                .setMax(pulsarClient.getConfiguration().getMaxBackoffIntervalNanos(), TimeUnit.NANOSECONDS)
                .setMandatoryStop(100, TimeUnit.MILLISECONDS)
                .create(),
            this);
        this.connectFuture = connectFuture;
        this.internalPinnedExecutor = pulsarClient.getInternalExecutorService();
        this.timer = pulsarClient.timer();
    }

    public void start() {
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
    public CompletableFuture<Void> connectionOpened(ClientCnx cnx) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        internalPinnedExecutor.execute(() -> {
            LOG.info("Transaction meta handler with transaction coordinator id {} connection opened.",
                    transactionCoordinatorId);

            State state = getState();
            if (state == State.Closing || state == State.Closed) {
                setState(State.Closed);
                failPendingRequest();
                future.complete(null);
                return;
            }

            // if broker protocol version < 19, don't send TcClientConnectRequest to broker.
            if (cnx.getRemoteEndpointProtocolVersion() > ProtocolVersion.v18.getValue()) {
                long requestId = client.newRequestId();
                ByteBuf request = Commands.newTcClientConnectRequest(transactionCoordinatorId, requestId);

                cnx.sendRequestWithId(request, requestId).thenRun(() -> {
                    internalPinnedExecutor.execute(() -> {
                        LOG.info("Transaction coordinator client connect success! tcId : {}", transactionCoordinatorId);
                        if (registerToConnection(cnx)) {
                            this.connectionHandler.resetBackoff();
                            pendingRequests.forEach((requestID, opBase) -> checkStateAndSendRequest(opBase));
                        }
                        future.complete(null);
                    });
                }).exceptionally((e) -> {
                    internalPinnedExecutor.execute(() -> {
                        LOG.error("Transaction coordinator client connect fail! tcId : {}",
                                transactionCoordinatorId, e.getCause());
                        if (getState() == State.Closing || getState() == State.Closed
                                || e.getCause() instanceof PulsarClientException.NotAllowedException) {
                            setState(State.Closed);
                            cnx.channel().close();
                            future.complete(null);
                        } else {
                            future.completeExceptionally(e.getCause());
                        }
                    });
                    return null;
                });
            } else {
                LOG.warn("Can not connect to the transaction coordinator because the protocol version {} is "
                                + "lower than 19", cnx.getRemoteEndpointProtocolVersion());
                registerToConnection(cnx);
                future.complete(null);
            }
        });
        return future;
    }

    private boolean registerToConnection(ClientCnx cnx) {
        if (changeToReadyState()) {
            connectionHandler.setClientCnx(cnx);
            cnx.registerTransactionMetaStoreHandler(transactionCoordinatorId, this);
            connectFuture.complete(null);
            return true;
        } else {
            State state = getState();
            cnx.channel().close();
            connectFuture.completeExceptionally(
                    new IllegalStateException("Failed to change the state from " + state + " to Ready"));
            return false;
        }
    }

    private void failPendingRequest() {
        // this method is executed in internalPinnedExecutor.
        pendingRequests.forEach((k, op) -> {
            if (op != null && !op.callback.isDone()) {
                op.callback.completeExceptionally(new PulsarClientException.AlreadyClosedException(
                        "Could not get response from transaction meta store when "
                                + "the transaction meta store has already close."));
                onResponse(op);
            }
        });
        this.pendingRequests.clear();
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
        OpForTxnIdCallBack op = OpForTxnIdCallBack.create(cmd, callback, client);
        internalPinnedExecutor.execute(() -> {
            pendingRequests.put(requestId, op);
            timeoutQueue.add(new RequestTime(System.currentTimeMillis(), requestId));
            if (!checkStateAndSendRequest(op)) {
                pendingRequests.remove(requestId);
            }
        });
        return callback;
    }

    void handleNewTxnResponse(CommandNewTxnResponse response) {
        boolean hasError = response.hasError();
        ServerError error;
        String message;
        if (hasError) {
             error = response.getError();
             message = response.getMessage();
        } else {
            error = null;
            message = null;
        }
        TxnID txnID = new TxnID(response.getTxnidMostBits(), response.getTxnidLeastBits());
        long requestId = response.getRequestId();
        internalPinnedExecutor.execute(() -> {
            OpForTxnIdCallBack op = (OpForTxnIdCallBack) pendingRequests.remove(requestId);
            if (op == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Got new txn response for timeout {} - {}", txnID.getMostSigBits(),
                            txnID.getLeastSigBits());
                }
                return;
            }

            if (!hasError) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Got new txn response {} for request {}", txnID, requestId);
                }
                op.callback.complete(txnID);
            } else {
                if (checkIfNeedRetryByError(error, message, op)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Get a response for the {}  request {} error "
                                        + "TransactionCoordinatorNotFound and try it again",
                                BaseCommand.Type.NEW_TXN.name(), requestId);
                    }
                    pendingRequests.put(requestId, op);
                    timer.newTimeout(timeout -> {
                                internalPinnedExecutor.execute(() -> {
                                    if (!pendingRequests.containsKey(requestId)) {
                                        if (LOG.isDebugEnabled()) {
                                            LOG.debug("The request {} already timeout", requestId);
                                        }
                                        return;
                                    }
                                    if (!checkStateAndSendRequest(op)) {
                                        pendingRequests.remove(requestId);
                                    }
                                });
                            }
                            , op.backoff.next(), TimeUnit.MILLISECONDS);
                    return;
                }
                LOG.error("Got {} for request {} error {}", BaseCommand.Type.NEW_TXN.name(),
                        requestId, error);
            }

            onResponse(op);
        });
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
        OpForVoidCallBack op = OpForVoidCallBack
                .create(cmd, callback, client);
        internalPinnedExecutor.execute(() -> {
            pendingRequests.put(requestId, op);
            timeoutQueue.add(new RequestTime(System.currentTimeMillis(), requestId));
            if (!checkStateAndSendRequest(op)) {
                pendingRequests.remove(requestId);
            }
        });

        return callback;
    }

    void handleAddPublishPartitionToTxnResponse(CommandAddPartitionToTxnResponse response) {
        boolean hasError = response.hasError();
        ServerError error;
        String message;
        if (hasError) {
            error = response.getError();
            message = response.getMessage();
        } else {
            error = null;
            message = null;
        }
        TxnID txnID = new TxnID(response.getTxnidMostBits(), response.getTxnidLeastBits());
        long requestId = response.getRequestId();
        internalPinnedExecutor.execute(() -> {
            OpForVoidCallBack op = (OpForVoidCallBack) pendingRequests.remove(requestId);
            if (op == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Got add publish partition to txn response for timeout {} - {}", txnID.getMostSigBits(),
                            txnID.getLeastSigBits());
                }
                return;
            }

            if (!hasError) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Add publish partition for request {} success.", requestId);
                }
                op.callback.complete(null);
            } else {
                if (checkIfNeedRetryByError(error, message, op)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Get a response for the {} request {} "
                                        + " error TransactionCoordinatorNotFound and try it again",
                                BaseCommand.Type.ADD_PARTITION_TO_TXN.name(), requestId);
                    }
                    pendingRequests.put(requestId, op);
                    timer.newTimeout(timeout -> {
                                internalPinnedExecutor.execute(() -> {
                                    if (!pendingRequests.containsKey(requestId)) {
                                        if (LOG.isDebugEnabled()) {
                                            LOG.debug("The request {} already timeout", requestId);
                                        }
                                        return;
                                    }
                                    if (!checkStateAndSendRequest(op)) {
                                        pendingRequests.remove(requestId);
                                    }
                                });
                            }
                            , op.backoff.next(), TimeUnit.MILLISECONDS);
                    return;
                }
                LOG.error("{} for request {} error {} with txnID {}.", BaseCommand.Type.ADD_PARTITION_TO_TXN.name(),
                        requestId, error, txnID);

            }

            onResponse(op);
        });
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
        OpForVoidCallBack op = OpForVoidCallBack.create(cmd, callback, client);
        internalPinnedExecutor.execute(() -> {
            pendingRequests.put(requestId, op);
            timeoutQueue.add(new RequestTime(System.currentTimeMillis(), requestId));
            if (!checkStateAndSendRequest(op)) {
                pendingRequests.remove(requestId);
            }
        });
        return callback;
    }

    public void handleAddSubscriptionToTxnResponse(CommandAddSubscriptionToTxnResponse response) {
        boolean hasError = response.hasError();
        ServerError error;
        String message;
        if (hasError) {
            error = response.getError();
            message = response.getMessage();
        } else {
            error = null;
            message = null;
        }
        long requestId = response.getRequestId();
        internalPinnedExecutor.execute(() -> {
            OpForVoidCallBack op = (OpForVoidCallBack) pendingRequests.remove(requestId);
            if (op == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Add subscription to txn timeout for request {}.", requestId);
                }
                return;
            }

            if (!hasError) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Add subscription to txn success for request {}.", requestId);
                }
                op.callback.complete(null);
            } else {
                LOG.error("Add subscription to txn failed for request {} error {}.",
                        requestId, error);
                if (checkIfNeedRetryByError(error, message, op)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Get a response for {} request {} error TransactionCoordinatorNotFound and try it"
                                        + " again", BaseCommand.Type.ADD_SUBSCRIPTION_TO_TXN.name(), requestId);
                    }
                    pendingRequests.put(requestId, op);
                    timer.newTimeout(timeout -> {
                                internalPinnedExecutor.execute(() -> {
                                    if (!pendingRequests.containsKey(requestId)) {
                                        if (LOG.isDebugEnabled()) {
                                            LOG.debug("The request {} already timeout", requestId);
                                        }
                                        return;
                                    }
                                    if (!checkStateAndSendRequest(op)) {
                                        pendingRequests.remove(requestId);
                                    }
                                });
                            }
                            , op.backoff.next(), TimeUnit.MILLISECONDS);
                    return;
                }
                LOG.error("{} failed for request {} error {}.", BaseCommand.Type.ADD_SUBSCRIPTION_TO_TXN.name(),
                       requestId, error);

            }
            onResponse(op);
        });
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
        OpForVoidCallBack op = OpForVoidCallBack.create(buf, callback, client);
        internalPinnedExecutor.execute(() -> {
            pendingRequests.put(requestId, op);
            timeoutQueue.add(new RequestTime(System.currentTimeMillis(), requestId));
            if (!checkStateAndSendRequest(op)) {
                pendingRequests.remove(requestId);
            }
        });
        return callback;
    }

    void handleEndTxnResponse(CommandEndTxnResponse response) {
        boolean hasError = response.hasError();
        ServerError error;
        String message;
        if (hasError) {
            error = response.getError();
            message = response.getMessage();
        } else {
            error = null;
            message = null;
        }
        TxnID txnID = new TxnID(response.getTxnidMostBits(), response.getTxnidLeastBits());
        long requestId = response.getRequestId();
        internalPinnedExecutor.execute(() -> {
            OpForVoidCallBack op = (OpForVoidCallBack) pendingRequests.remove(requestId);
            if (op == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Got end txn response for timeout {} - {}", txnID.getMostSigBits(),
                            txnID.getLeastSigBits());
                }
                return;
            }

            if (!hasError) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Got end txn response success for request {}", requestId);
                }
                op.callback.complete(null);
            } else {
                if (checkIfNeedRetryByError(error, message, op)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Get a response for the {} request {} error "
                                        + "TransactionCoordinatorNotFound and try it again",
                                BaseCommand.Type.END_TXN.name(), requestId);
                    }
                    pendingRequests.put(requestId, op);
                    timer.newTimeout(timeout -> {
                                internalPinnedExecutor.execute(() -> {
                                    if (!pendingRequests.containsKey(requestId)) {
                                        if (LOG.isDebugEnabled()) {
                                            LOG.debug("The request {} already timeout", requestId);
                                        }
                                        return;
                                    }
                                    if (!checkStateAndSendRequest(op)) {
                                        pendingRequests.remove(requestId);
                                    }
                                });
                            }
                            , op.backoff.next(), TimeUnit.MILLISECONDS);
                    return;
                }
                LOG.error("Got {} response for request {} error {}", BaseCommand.Type.END_TXN.name(),
                        requestId, error);

            }
            onResponse(op);
        });
    }


    private boolean checkIfNeedRetryByError(ServerError error, String message, OpBase<?> op) {
        if (error == ServerError.TransactionCoordinatorNotFound) {
            if (getState() != State.Connecting) {
                connectionHandler.reconnectLater(new TransactionCoordinatorClientException
                        .CoordinatorNotFoundException(message));
            }
            return true;
        }

        if (op != null) {
            op.callback.completeExceptionally(getExceptionByServerError(error, message));
        }
        return false;
    }

    private abstract static class OpBase<T> {
        protected ByteBuf cmd;
        protected CompletableFuture<T> callback;
        protected Backoff backoff;

        abstract void recycle();
    }

    private static class OpForTxnIdCallBack extends OpBase<TxnID> {

        static OpForTxnIdCallBack create(ByteBuf cmd, CompletableFuture<TxnID> callback, PulsarClientImpl client) {
            OpForTxnIdCallBack op = RECYCLER.get();
            op.callback = callback;
            op.cmd = cmd;
            op.backoff = new BackoffBuilder()
                    .setInitialTime(client.getConfiguration().getInitialBackoffIntervalNanos(),
                            TimeUnit.NANOSECONDS)
                    .setMax(client.getConfiguration().getMaxBackoffIntervalNanos() / 10, TimeUnit.NANOSECONDS)
                    .setMandatoryStop(0, TimeUnit.MILLISECONDS)
                    .create();
            return op;
        }

        private OpForTxnIdCallBack(Recycler.Handle<OpForTxnIdCallBack> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        @Override
        void recycle() {
            this.backoff = null;
            this.cmd = null;
            this.callback = null;
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


        static OpForVoidCallBack create(ByteBuf cmd, CompletableFuture<Void> callback, PulsarClientImpl client) {
            OpForVoidCallBack op = RECYCLER.get();
            op.callback = callback;
            op.cmd = cmd;
            op.backoff = new BackoffBuilder()
                    .setInitialTime(client.getConfiguration().getInitialBackoffIntervalNanos(),
                            TimeUnit.NANOSECONDS)
                    .setMax(client.getConfiguration().getMaxBackoffIntervalNanos() / 10, TimeUnit.NANOSECONDS)
                    .setMandatoryStop(0, TimeUnit.MILLISECONDS)
                    .create();
            return op;
        }

        private OpForVoidCallBack(Recycler.Handle<OpForVoidCallBack> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        @Override
        void recycle() {
            this.backoff = null;
            this.cmd = null;
            this.callback = null;
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

    private boolean checkStateAndSendRequest(OpBase<?> op) {
        switch (getState()) {
            case Ready:
                ClientCnx cnx = cnx();
                if (cnx != null) {
                    op.cmd.retain();
                    cnx.ctx().writeAndFlush(op.cmd, cnx().ctx().voidPromise());
                } else {
                    LOG.error("The cnx was null when the TC handler was ready", new NullPointerException());
                }
                return true;
            case Connecting:
                return true;
            case Closing:
            case Closed:
                op.callback.completeExceptionally(
                        new TransactionCoordinatorClientException.MetaStoreHandlerNotReadyException(
                                "Transaction meta store handler for tcId "
                                        + transactionCoordinatorId
                                        + " is closing or closed."));
                onResponse(op);
                return false;
            case Failed:
            case Uninitialized:
                op.callback.completeExceptionally(
                        new TransactionCoordinatorClientException.MetaStoreHandlerNotReadyException(
                                "Transaction meta store handler for tcId "
                                        + transactionCoordinatorId
                                        + " not connected."));
                onResponse(op);
                return false;
            default:
                op.callback.completeExceptionally(
                        new TransactionCoordinatorClientException.MetaStoreHandlerNotReadyException(
                                transactionCoordinatorId));
                onResponse(op);
                return false;
        }
    }

    @Override
    public void run(Timeout timeout) throws Exception {
        internalPinnedExecutor.execute(() -> {
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
                long diff = (peeked.creationTimeMs + client.getConfiguration().getOperationTimeoutMs())
                        - System.currentTimeMillis();
                if (diff <= 0) {
                    timeToWaitMs = client.getConfiguration().getOperationTimeoutMs();
                } else {
                    timeToWaitMs = diff;
                }
            }
            requestTimeout = client.timer().newTimeout(this, timeToWaitMs, TimeUnit.MILLISECONDS);
        });
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
