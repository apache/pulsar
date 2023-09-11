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
package org.apache.pulsar.broker.transaction.buffer.impl;

import io.netty.buffer.ByteBuf;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCountUtil;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceEphemeralData;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.TransactionBufferClientException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.transaction.TransactionBufferHandler;
import org.apache.pulsar.common.api.proto.CommandEndTxnOnPartitionResponse;
import org.apache.pulsar.common.api.proto.CommandEndTxnOnSubscriptionResponse;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.GrowableArrayBlockingQueue;

@Slf4j
public class TransactionBufferHandlerImpl implements TransactionBufferHandler {

    private final ConcurrentSkipListMap<Long, OpRequestSend> outstandingRequests;
    private final GrowableArrayBlockingQueue<OpRequestSend> pendingRequests;
    private final AtomicLong requestIdGenerator = new AtomicLong();
    private final long operationTimeoutInMills;
    private final HashedWheelTimer timer;
    private final PulsarService pulsarService;
    private final PulsarClientImpl pulsarClient;

    private final int randomKeyForSelectConnection;

    private static final AtomicIntegerFieldUpdater<TransactionBufferHandlerImpl> REQUEST_CREDITS_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(TransactionBufferHandlerImpl.class, "requestCredits");
    private volatile int requestCredits;

    public TransactionBufferHandlerImpl(PulsarService pulsarService, HashedWheelTimer timer,
        int maxConcurrentRequests, long operationTimeoutInMills) throws PulsarServerException {
        this.pulsarService = pulsarService;
        this.pulsarClient = (PulsarClientImpl) pulsarService.getClient();
        this.outstandingRequests = new ConcurrentSkipListMap<>();
        this.pendingRequests = new GrowableArrayBlockingQueue<>();
        this.operationTimeoutInMills = operationTimeoutInMills;
        this.timer = timer;
        this.requestCredits = Math.max(100, maxConcurrentRequests);
        this.randomKeyForSelectConnection = pulsarClient.getCnxPool().genRandomKeyToSelectCon();
    }

    @Override
    public CompletableFuture<TxnID> endTxnOnTopic(String topic, long txnIdMostBits, long txnIdLeastBits,
                                                  TxnAction action, long lowWaterMark) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] endTxnOnTopic txnId: [{}], txnAction: [{}]",
                    topic, new TxnID(txnIdMostBits, txnIdLeastBits), action.getValue());
        }
        CompletableFuture<TxnID> cb = new CompletableFuture<>();
        long requestId = requestIdGenerator.getAndIncrement();
        ByteBuf cmd = Commands.newEndTxnOnPartition(requestId, txnIdLeastBits, txnIdMostBits,
                topic, action, lowWaterMark);

        OpRequestSend op = OpRequestSend.create(requestId, topic, cmd, cb, getClientCnx(topic));
        if (checkRequestCredits(op)) {
            endTxn(op);
        }
        return cb;
    }

    @Override
    public CompletableFuture<TxnID> endTxnOnSubscription(String topic, String subscription,
                                                                      long txnIdMostBits, long txnIdLeastBits,
                                                                      TxnAction action, long lowWaterMark) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] endTxnOnSubscription txnId: [{}], txnAction: [{}]",
                    topic, new TxnID(txnIdMostBits, txnIdLeastBits), action.getValue());
        }
        CompletableFuture<TxnID> cb = new CompletableFuture<>();
        long requestId = requestIdGenerator.getAndIncrement();
        ByteBuf cmd = Commands.newEndTxnOnSubscription(requestId, txnIdLeastBits, txnIdMostBits,
                topic, subscription, action, lowWaterMark);
        OpRequestSend op = OpRequestSend.create(requestId, topic, cmd, cb, getClientCnx(topic));
        if (checkRequestCredits(op)) {
            endTxn(op);
        }
        return cb;
    }

    private boolean checkRequestCredits(OpRequestSend op) {
        int currentPermits = REQUEST_CREDITS_UPDATER.get(this);
        if (currentPermits > 0 && pendingRequests.peek() == null) {
            if (REQUEST_CREDITS_UPDATER.compareAndSet(this, currentPermits, currentPermits - 1)) {
                return true;
            } else {
                return checkRequestCredits(op);
            }
        } else {
            pendingRequests.add(op);
            return false;
        }
    }

    public void endTxn(OpRequestSend op) {
        op.cnx.whenComplete((clientCnx, ex) -> {
            if (ex == null) {
                if (clientCnx.ctx().channel().isActive()) {
                    clientCnx.registerTransactionBufferHandler(TransactionBufferHandlerImpl.this);
                    outstandingRequests.put(op.requestId, op);
                    timer.newTimeout(timeout -> {
                        OpRequestSend peek = outstandingRequests.remove(op.requestId);
                        if (peek != null && !peek.cb.isDone() && !peek.cb.isCompletedExceptionally()) {
                            peek.cb.completeExceptionally(new TransactionBufferClientException
                                    .RequestTimeoutException());
                            onResponse(peek);
                        }
                    }, operationTimeoutInMills, TimeUnit.MILLISECONDS);
                    op.cmd.retain();
                    clientCnx.ctx().writeAndFlush(op.cmd, clientCnx.ctx().voidPromise());
                } else {
                    op.cb.completeExceptionally(
                            new PulsarClientException.LookupException(op.topic + " endTxn channel is not active"));
                    onResponse(op);
                }
            } else {
                Throwable cause = FutureUtil.unwrapCompletionException(ex);
                log.error("endTxn error topic: [{}]", op.topic, cause);
                if (cause instanceof PulsarClientException.BrokerMetadataException) {
                    op.cb.complete(null);
                } else {
                    op.cb.completeExceptionally(
                            new PulsarClientException.LookupException(cause.getMessage()));
                }
                onResponse(op);
            }
        });
    }

    @Override
    public void handleEndTxnOnTopicResponse(long requestId, CommandEndTxnOnPartitionResponse response) {
        OpRequestSend op = outstandingRequests.remove(requestId);
        if (op == null) {
            if (log.isDebugEnabled()) {
                log.debug("Got end txn on topic response for timeout {} - {}", response.getTxnidMostBits(),
                        response.getTxnidLeastBits());
            }
            return;
        }
        try {
            if (!response.hasError()) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Got end txn on topic response for for request {}", op.topic,
                            response.getRequestId());
                }
                op.cb.complete(new TxnID(response.getTxnidMostBits(), response.getTxnidLeastBits()));
            } else {
                log.error("[{}] Got end txn on topic response for request {} error {}", op.topic,
                        response.getRequestId(),
                        response.getError());
                op.cb.completeExceptionally(ClientCnx.getPulsarClientException(response.getError(),
                        response.getMessage()));
            }
        } finally {
            onResponse(op);
        }
    }

    @Override
    public void handleEndTxnOnSubscriptionResponse(long requestId,
                                                   CommandEndTxnOnSubscriptionResponse response) {
        OpRequestSend op = outstandingRequests.remove(requestId);
        if (op == null) {
            if (log.isDebugEnabled()) {
                log.debug("Got end txn on subscription response for timeout {} - {}", response.getTxnidMostBits(),
                        response.getTxnidLeastBits());
            }
            return;
        }

        try {
            if (!response.hasError()) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Got end txn on subscription response for for request {}",
                            op.topic, response.getRequestId());
                }
                op.cb.complete(new TxnID(response.getTxnidMostBits(), response.getTxnidLeastBits()));
            } else {
                log.error("[{}] Got end txn on subscription response for request {} error {}",
                        op.topic, response.getRequestId(), response.getError());
                op.cb.completeExceptionally(ClientCnx.getPulsarClientException(response.getError(),
                        response.getMessage()));
            }
        } finally {
            onResponse(op);
        }
    }

    public void onResponse(OpRequestSend op) {
        REQUEST_CREDITS_UPDATER.incrementAndGet(this);
        if (op != null) {
            ReferenceCountUtil.safeRelease(op.cmd);
            op.recycle();
        }
        checkPendingRequests();
    }

    private void checkPendingRequests() {
        while (true) {
            int permits = REQUEST_CREDITS_UPDATER.get(this);
            if (permits > 0 && pendingRequests.peek() != null) {
                if (REQUEST_CREDITS_UPDATER.compareAndSet(this, permits, permits - 1)) {
                    OpRequestSend polled = pendingRequests.poll();
                    if (polled != null) {
                        CompletableFuture<ClientCnx> clientCnx = getClientCnx(polled.topic);
                        if (polled.cnx != clientCnx) {
                            OpRequestSend invalid = polled;
                            polled = OpRequestSend.create(invalid.requestId, invalid.topic, invalid.cmd, invalid.cb,
                                    clientCnx);
                            invalid.recycle();
                        }
                        endTxn(polled);
                    } else {
                        REQUEST_CREDITS_UPDATER.incrementAndGet(this);
                    }
                }
            } else {
                break;
            }
        }
    }

    public static final class OpRequestSend {

        long requestId;
        String topic;
        ByteBuf cmd;
        CompletableFuture<TxnID> cb;
        long createdAt;
        CompletableFuture<ClientCnx> cnx;

        static OpRequestSend create(long requestId, String topic, ByteBuf cmd, CompletableFuture<TxnID> cb,
                CompletableFuture<ClientCnx> cnx) {
            OpRequestSend op = RECYCLER.get();
            op.requestId = requestId;
            op.topic = topic;
            op.cmd = cmd;
            op.cb = cb;
            op.createdAt = System.currentTimeMillis();
            op.cnx = cnx;
            return op;
        }

        void recycle() {
            recyclerHandle.recycle(this);
        }

        private OpRequestSend(Recycler.Handle<OpRequestSend> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private final Recycler.Handle<OpRequestSend> recyclerHandle;

        private static final Recycler<OpRequestSend> RECYCLER = new Recycler<OpRequestSend>() {
            @Override
            protected OpRequestSend newObject(Handle<OpRequestSend> handle) {
                return new OpRequestSend(handle);
            }
        };
    }

    public CompletableFuture<ClientCnx> getClientCnxWithLookup(String topic) {
        return pulsarClient.getConnection(topic, randomKeyForSelectConnection);
    }

    public CompletableFuture<ClientCnx> getClientCnx(String topic) {
        NamespaceService namespaceService = pulsarService.getNamespaceService();
        CompletableFuture<NamespaceBundle> nsBundle = namespaceService.getBundleAsync(TopicName.get(topic));
        return nsBundle
                .thenCompose(bundle -> namespaceService.getOwnerAsync(bundle))
                .thenCompose(data -> {
                    if (data.isPresent()) {
                        NamespaceEphemeralData ephemeralData = data.get();
                        try {
                            if (!ephemeralData.isDisabled()) {
                                URI uri;
                                if (pulsarClient.getConfiguration().isUseTls()) {
                                    uri = new URI(ephemeralData.getNativeUrlTls());
                                } else {
                                    uri = new URI(ephemeralData.getNativeUrl());
                                }
                                InetSocketAddress brokerAddress =
                                        InetSocketAddress.createUnresolved(uri.getHost(), uri.getPort());
                                return pulsarClient.getConnection(brokerAddress, brokerAddress,
                                        randomKeyForSelectConnection);
                            } else {
                                // Bundle is unloading, lookup topic
                                return getClientCnxWithLookup(topic);
                            }
                        } catch (URISyntaxException e) {
                            // Should never go here
                            return getClientCnxWithLookup(topic);
                        }
                    } else {
                        // Bundle is not loaded yet, lookup topic
                        return getClientCnxWithLookup(topic);
                    }
                });
    }

    @Override
    public void close() {
        this.timer.stop();
    }

    @Override
    public int getAvailableRequestCredits() {
        return REQUEST_CREDITS_UPDATER.get(this);
    }

    @Override
    public int getPendingRequestsCount() {
        return pendingRequests.size();
    }
}
