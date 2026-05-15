/*
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
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.CustomLog;
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
import org.apache.pulsar.common.util.Backoff;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;

/**
 * Handler for transaction meta store.
 */
@CustomLog
public class TransactionMetaStoreHandler extends HandlerState
        implements ConnectionHandler.Connection, Closeable, TimerTask {

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
    private final long lookupDeadline;
    private final AtomicInteger previousExceptionCount = new AtomicInteger();



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
            Backoff.builder()
                .initialDelay(Duration.ofNanos(pulsarClient.getConfiguration()
                        .getInitialBackoffIntervalNanos()))
                .maxBackoff(Duration.ofNanos(pulsarClient.getConfiguration().getMaxBackoffIntervalNanos()))
                .mandatoryStop(Duration.ofMillis(100))
                .build(),
            this);
        this.connectFuture = connectFuture;
        this.internalPinnedExecutor = pulsarClient.getInternalExecutorService();
        this.timer = pulsarClient.timer();
        this.lookupDeadline = System.currentTimeMillis() + client.getConfiguration().getLookupTimeoutMs();
    }

    public void start() {
        this.connectionHandler.grabCnx();
    }

    @Override
    public boolean connectionFailed(PulsarClientException exception) {
        boolean nonRetriableError = !PulsarClientException.isRetriableError(exception);
        boolean timeout = System.currentTimeMillis() > lookupDeadline;
        if (nonRetriableError || timeout) {
            exception.setPreviousExceptionCount(previousExceptionCount);
            if (connectFuture.completeExceptionally(exception)) {
                if (nonRetriableError) {
                    log.error().attr("transactionCoordinatorId", transactionCoordinatorId)
                            .exception(exception)
                            .log("Transaction meta handler failed.");
                } else {
                    log.error().attr("transactionCoordinatorId", transactionCoordinatorId)
                            .exception(exception)
                            .log("Transaction meta handler with transaction"
                                    + " coordinator id connection failed"
                                    + " after timeout");
                }
                setState(State.Failed);
                return false;
            }
        } else {
            previousExceptionCount.getAndIncrement();
        }
        return true;
    }

    @Override
    public CompletableFuture<Void> connectionOpened(ClientCnx cnx) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        internalPinnedExecutor.execute(() -> {
            log.info().attr("transactionCoordinatorId", transactionCoordinatorId)
                    .log("Transaction meta handler with transaction coordinator id connection opened.");

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
                        log.info().attr("transactionCoordinatorId", transactionCoordinatorId)
                                .log("Transaction coordinator client connect success! tcId");
                        if (registerToConnection(cnx)) {
                            this.connectionHandler.resetBackoff();
                            pendingRequests.forEach((requestID, opBase) -> checkStateAndSendRequest(opBase));
                        }
                        future.complete(null);
                    });
                }).exceptionally((e) -> {
                    internalPinnedExecutor.execute(() -> {
                        log.error().attr("transactionCoordinatorId", transactionCoordinatorId)
                                .exception(e.getCause())
                                .log("Transaction coordinator client connect fail! tcId");
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
                log.warn().attr("version", cnx.getRemoteEndpointProtocolVersion())
                        .log("Can not connect to the transaction coordinator"
                                + " because the protocol version is"
                                + " lower than 19");
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
            log.debug().attr("timeoutMs", unit.toMillis(timeout)).log("New transaction with timeout");
        CompletableFuture<TxnID> callback = new CompletableFuture<>();
        if (!canSendRequest(callback)) {
            return callback;
        }
        long requestId = client.newRequestId();
        ByteBuf cmd = Commands.newTxn(transactionCoordinatorId, requestId, unit.toMillis(timeout));
        String description = String.format("Create new transaction %s", transactionCoordinatorId);
        OpForTxnIdCallBack op = OpForTxnIdCallBack.create(cmd, callback, client, description, cnx());
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
        final boolean hasError = response.hasError();
        final ServerError error;
        final String message;
        if (hasError) {
             error = response.getError();
             message = response.getMessage();
        } else {
            error = null;
            message = null;
        }
        final TxnID txnID = new TxnID(response.getTxnidMostBits(), response.getTxnidLeastBits());
        final long requestId = response.getRequestId();
        internalPinnedExecutor.execute(() -> {
            OpForTxnIdCallBack op = (OpForTxnIdCallBack) pendingRequests.remove(requestId);
            if (op == null) {
                    log.debug().attr("transaction", txnID).log("Got new txn response for transaction");
                return;
            }

            if (!hasError) {
                    log.debug().attr("response", txnID)
                            .attr("request", requestId)
                            .log("Got new txn response for request");
                op.callback.complete(txnID);
            } else {
                if (checkIfNeedRetryByError(error, message, op)) {
                        log.debug().attr("name", BaseCommand.Type.NEW_TXN)
                                .attr("requestId", requestId)
                                .log("Get a response for the request error"
                                        + " TransactionCoordinatorNotFound"
                                        + " and try it again");
                    pendingRequests.put(requestId, op);
                    timer.newTimeout(timeout -> {
                                internalPinnedExecutor.execute(() -> {
                                    if (!pendingRequests.containsKey(requestId)) {
                                            log.debug().attr("request", requestId).log("The request already timeout");
                                        return;
                                    }
                                    if (!checkStateAndSendRequest(op)) {
                                        pendingRequests.remove(requestId);
                                    }
                                });
                            }
                            , op.backoff.next().toMillis(), TimeUnit.MILLISECONDS);
                    return;
                }
                log.error().attr("name", BaseCommand.Type.NEW_TXN)
                        .attr("requestId", requestId)
                        .attr("error", error)
                        .log("Got for request error");
            }

            onResponse(op);
        });
    }

    public CompletableFuture<Void> addPublishPartitionToTxnAsync(TxnID txnID, List<String> partitions) {
            log.debug().attr("partition", partitions).attr("txn", txnID).log("Add publish partition to txn");
        CompletableFuture<Void> callback = new CompletableFuture<>();
        if (!canSendRequest(callback)) {
            return callback;
        }
        long requestId = client.newRequestId();
        ByteBuf cmd = Commands.newAddPartitionToTxn(
                requestId, txnID.getLeastSigBits(), txnID.getMostSigBits(), partitions);
        String description = String.format("Add partition %s to TXN %s", String.valueOf(partitions),
                String.valueOf(txnID));
        OpForVoidCallBack op = OpForVoidCallBack
                .create(cmd, callback, client, description, cnx());
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
        final boolean hasError = response.hasError();
        final ServerError error;
        final String message;
        if (hasError) {
            error = response.getError();
            message = response.getMessage();
        } else {
            error = null;
            message = null;
        }
        final TxnID txnID = new TxnID(response.getTxnidMostBits(), response.getTxnidLeastBits());
        final long requestId = response.getRequestId();
        internalPinnedExecutor.execute(() -> {
            OpForVoidCallBack op = (OpForVoidCallBack) pendingRequests.remove(requestId);
            if (op == null) {
                    log.debug().attr("transaction", txnID)
                            .log("Got add publish partition to txn response for transaction");
                return;
            }

            if (!hasError) {
                    log.debug().attr("request", requestId).log("Add publish partition for request success.");
                op.callback.complete(null);
            } else {
                if (checkIfNeedRetryByError(error, message, op)) {
                        log.debug().attr("name", BaseCommand.Type.ADD_PARTITION_TO_TXN.name())
                                .attr("request", requestId)
                                .log("Get a response for the request"
                                        + " error TransactionCoordinatorNotFound"
                                        + " and try it again");
                    pendingRequests.put(requestId, op);
                    timer.newTimeout(timeout -> {
                                internalPinnedExecutor.execute(() -> {
                                    if (!pendingRequests.containsKey(requestId)) {
                                            log.debug().attr("request", requestId).log("The request already timeout");
                                        return;
                                    }
                                    if (!checkStateAndSendRequest(op)) {
                                        pendingRequests.remove(requestId);
                                    }
                                });
                            }
                            , op.backoff.next().toMillis(), TimeUnit.MILLISECONDS);
                    return;
                }
                log.error().attr("name", BaseCommand.Type.ADD_PARTITION_TO_TXN.name())
                        .attr("request", requestId)
                        .attr("transaction", txnID)
                        .attr("error", error)
                        .log("for request, transaction, error");

            }

            onResponse(op);
        });
    }

    public CompletableFuture<Void> addSubscriptionToTxn(TxnID txnID, List<Subscription> subscriptionList) {
            log.debug().attr("subscription", subscriptionList).attr("txn", txnID).log("Add subscription to txn.");

        CompletableFuture<Void> callback = new CompletableFuture<>();
        if (!canSendRequest(callback)) {
            return callback;
        }
        long requestId = client.newRequestId();
        ByteBuf cmd = Commands.newAddSubscriptionToTxn(
                requestId, txnID.getLeastSigBits(), txnID.getMostSigBits(), subscriptionList);
        String description = String.format("Add subscription %s to TXN %s", toStringSubscriptionList(subscriptionList),
                String.valueOf(txnID));
        OpForVoidCallBack op = OpForVoidCallBack.create(cmd, callback, client, description, cnx());
        internalPinnedExecutor.execute(() -> {
            pendingRequests.put(requestId, op);
            timeoutQueue.add(new RequestTime(System.currentTimeMillis(), requestId));
            if (!checkStateAndSendRequest(op)) {
                pendingRequests.remove(requestId);
            }
        });
        return callback;
    }

    private String toStringSubscriptionList(List<Subscription> list) {
        if (list == null || list.isEmpty()) {
            return "[]";
        }
        StringBuilder builder = new StringBuilder("[");
        for (Subscription subscription : list) {
            builder.append(String.format("%s %s", subscription.getTopic(), subscription.getSubscription()));
        }
        return builder.append("]").toString();
    }

    public void handleAddSubscriptionToTxnResponse(CommandAddSubscriptionToTxnResponse response) {
        final boolean hasError = response.hasError();
        final ServerError error;
        final String message;
        if (hasError) {
            error = response.getError();
            message = response.getMessage();
        } else {
            error = null;
            message = null;
        }
        final long requestId = response.getRequestId();
        final TxnID txnID = new TxnID(response.getTxnidMostBits(), response.getTxnidLeastBits());
        internalPinnedExecutor.execute(() -> {
            OpForVoidCallBack op = (OpForVoidCallBack) pendingRequests.remove(requestId);
            if (op == null) {
                    log.debug().attr("request", requestId).log("Add subscription to txn timeout for request.");
                return;
            }

            if (!hasError) {
                    log.debug().attr("request", requestId).log("Add subscription to txn success for request.");
                op.callback.complete(null);
            } else {
                log.error().attr("request", requestId)
                        .attr("transaction", txnID)
                        .attr("error", error)
                        .log("Add subscription to txn failed for request, transaction, error");
                if (checkIfNeedRetryByError(error, message, op)) {
                        log.debug().attr("name", BaseCommand.Type.ADD_SUBSCRIPTION_TO_TXN.name())
                                .attr("request", requestId)
                                .log("Get a response for request error"
                                        + " TransactionCoordinatorNotFound"
                                        + " and try it again");
                    pendingRequests.put(requestId, op);
                    timer.newTimeout(timeout -> {
                                internalPinnedExecutor.execute(() -> {
                                    if (!pendingRequests.containsKey(requestId)) {
                                            log.debug().attr("request", requestId).log("The request already timeout");
                                        return;
                                    }
                                    if (!checkStateAndSendRequest(op)) {
                                        pendingRequests.remove(requestId);
                                    }
                                });
                            }
                            , op.backoff.next().toMillis(), TimeUnit.MILLISECONDS);
                    return;
                }
                log.error().attr("name", BaseCommand.Type.ADD_SUBSCRIPTION_TO_TXN.name())
                        .attr("request", requestId)
                        .attr("error", error)
                        .log("failed for request error.");

            }
            onResponse(op);
        });
    }

    public CompletableFuture<Void> endTxnAsync(TxnID txnID, TxnAction action) {
            log.debug().attr("txn", txnID).attr("action", action).log("End txn, action");
        CompletableFuture<Void> callback = new CompletableFuture<>();
        if (!canSendRequest(callback)) {
            return callback;
        }
        long requestId = client.newRequestId();
        BaseCommand cmd = Commands.newEndTxn(requestId, txnID.getLeastSigBits(), txnID.getMostSigBits(), action);
        ByteBuf buf = Commands.serializeWithSize(cmd);
        String description = String.format("End [%s] TXN %s", String.valueOf(action), String.valueOf(txnID));
        OpForVoidCallBack op = OpForVoidCallBack.create(buf, callback, client, description, cnx());
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
        final boolean hasError = response.hasError();
        final ServerError error;
        final String message;
        if (hasError) {
            error = response.getError();
            message = response.getMessage();
        } else {
            error = null;
            message = null;
        }
        final TxnID txnID = new TxnID(response.getTxnidMostBits(), response.getTxnidLeastBits());
        final long requestId = response.getRequestId();
        internalPinnedExecutor.execute(() -> {
            OpForVoidCallBack op = (OpForVoidCallBack) pendingRequests.remove(requestId);
            if (op == null) {
                    log.debug().attr("txn", txnID)
                            .log("Got end txn response for transaction but no requests pending for txn");
                return;
            }

            if (!hasError) {
                    log.debug().attr("request", requestId)
                            .attr("txn", txnID)
                            .log("Got end txn response success for request, txn");
                op.callback.complete(null);
            } else {
                if (checkIfNeedRetryByError(error, message, op)) {
                        log.debug().attr("name", BaseCommand.Type.END_TXN.name())
                                .attr("request", requestId)
                                .log("Get a response for the request error"
                                        + " TransactionCoordinatorNotFound"
                                        + " and try it again");
                    pendingRequests.put(requestId, op);
                    timer.newTimeout(timeout -> {
                                internalPinnedExecutor.execute(() -> {
                                    if (!pendingRequests.containsKey(requestId)) {
                                            log.debug().attr("request", requestId).log("The request already timeout");
                                        return;
                                    }
                                    if (!checkStateAndSendRequest(op)) {
                                        pendingRequests.remove(requestId);
                                    }
                                });
                            }
                            , op.backoff.next().toMillis(), TimeUnit.MILLISECONDS);
                    return;
                }
                log.error().attr("name", BaseCommand.Type.END_TXN.name())
                        .attr("request", requestId)
                        .attr("transaction", txnID)
                        .attr("error", error)
                        .log("Got response for request, transaction, error");

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
        protected String description;
        protected ClientCnx clientCnx;

        abstract void recycle();
    }

    private static class OpForTxnIdCallBack extends OpBase<TxnID> {

        static OpForTxnIdCallBack create(ByteBuf cmd, CompletableFuture<TxnID> callback, PulsarClientImpl client,
                                         String description, ClientCnx clientCnx) {
            OpForTxnIdCallBack op = RECYCLER.get();
            op.callback = callback;
            op.cmd = cmd;
            op.backoff = Backoff.builder()
                    .initialDelay(Duration.ofNanos(client.getConfiguration()
                            .getInitialBackoffIntervalNanos()))
                    .maxBackoff(Duration.ofNanos(client.getConfiguration().getMaxBackoffIntervalNanos() / 10))
                    .build();
            op.description = description;
            op.clientCnx = clientCnx;
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
            this.description = null;
            this.clientCnx = null;
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


        static OpForVoidCallBack create(ByteBuf cmd, CompletableFuture<Void> callback, PulsarClientImpl client,
                                        String description, ClientCnx clientCnx) {
            OpForVoidCallBack op = RECYCLER.get();
            op.callback = callback;
            op.cmd = cmd;
            op.backoff = Backoff.builder()
                    .initialDelay(Duration.ofNanos(client.getConfiguration()
                            .getInitialBackoffIntervalNanos()))
                    .maxBackoff(Duration.ofNanos(client.getConfiguration().getMaxBackoffIntervalNanos() / 10))
                    .build();
            op.description = description;
            op.clientCnx = clientCnx;
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
            this.description = null;
            this.clientCnx = null;
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
                    log.error().exception(new NullPointerException())
                            .log("The cnx was null when the TC handler was ready");
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
                            String.format("%s failed due to timeout. connection: %s. pending-queue: %s",
                                op.description, op.clientCnx, pendingRequests.size())));
                            log.debug().attr("request", lastPolled.requestId)
                                    .log("Transaction coordinator request is timeout.");
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
