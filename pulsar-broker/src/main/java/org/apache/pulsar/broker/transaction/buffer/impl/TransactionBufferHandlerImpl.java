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
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.client.api.transaction.TransactionBufferClientException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.client.impl.transaction.TransactionBufferHandler;
import org.apache.pulsar.common.util.FutureUtil;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class TransactionBufferHandlerImpl implements TransactionBufferHandler, TimerTask {

    private final ConcurrentSkipListMap<Long, OpRequestSend> pendingRequests;
    private final ConnectionPool connectionPool;
    private final NamespaceService namespaceService;
    private final AtomicLong requestIdGenerator = new AtomicLong();
    private long operationTimeoutInMills;
    private Timeout requestTimeout;
    private HashedWheelTimer timer;
    private final Semaphore semaphore;
    private final boolean blockIfReachMaxPendingOps;

    public TransactionBufferHandlerImpl(ConnectionPool connectionPool, NamespaceService namespaceService) {
        this.connectionPool = connectionPool;
        this.pendingRequests = new ConcurrentSkipListMap<>();
        this.namespaceService = namespaceService;
        this.operationTimeoutInMills = 3000L;
        this.semaphore = new Semaphore(10000);
        this.blockIfReachMaxPendingOps = true;
        this.timer = new HashedWheelTimer(new DefaultThreadFactory("pulsar-transaction-buffer-client-timer"));
        this.requestTimeout = timer.newTimeout(this, operationTimeoutInMills, TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletableFuture<TxnID> endTxnOnTopic(String topic, long txnIdMostBits, long txnIdLeastBits, PulsarApi.TxnAction action) {
        CompletableFuture<TxnID> cb = new CompletableFuture<>();
        if (!canSendRequest(cb)) {
            return cb;
        }
        long requestId = requestIdGenerator.getAndIncrement();
        ByteBuf cmd = Commands.newEndTxnOnPartition(requestId, txnIdLeastBits, txnIdMostBits, topic, action);
        OpRequestSend op = OpRequestSend.create(requestId, topic, cmd, cb);
        pendingRequests.put(requestId, op);
        cmd.retain();
        cnx(topic).whenComplete((clientCnx, throwable) -> {
            if (throwable == null) {
                try {
                    clientCnx.ctx().writeAndFlush(cmd, clientCnx.ctx().voidPromise());
                } catch (Exception e) {
                    cb.completeExceptionally(e);
                    pendingRequests.remove(requestId);
                    op.recycle();
                }
            } else {
                cb.completeExceptionally(throwable);
                pendingRequests.remove(requestId);
                op.recycle();
            }
        });
        return cb;
    }

    @Override
    public CompletableFuture<TxnID> endTxnOnSubscription(String topic, String subscription, long txnIdMostBits, long txnIdLeastBits, PulsarApi.TxnAction action) {
        CompletableFuture<TxnID> cb = new CompletableFuture<>();
        if (!canSendRequest(cb)) {
            return cb;
        }
        long requestId = requestIdGenerator.getAndIncrement();
        ByteBuf cmd = Commands.newEndTxnOnSubscription(requestId, txnIdLeastBits, txnIdMostBits, topic, subscription, action);
        OpRequestSend op = OpRequestSend.create(requestId, topic, cmd, cb);
        pendingRequests.put(requestId, op);
        cmd.retain();
        cnx(topic).whenComplete((clientCnx, throwable) -> {
            if (throwable == null) {
                try {
                    clientCnx.ctx().writeAndFlush(cmd, clientCnx.ctx().voidPromise());
                } catch (Exception e) {
                    cb.completeExceptionally(e);
                    pendingRequests.remove(requestId);
                    op.recycle();
                }
            } else {
                cb.completeExceptionally(throwable);
                pendingRequests.remove(requestId);
                op.recycle();
            }
        });
        return cb;
    }

    @Override
    public void handleEndTxnOnTopicResponse(long requestId, PulsarApi.CommandEndTxnOnPartitionResponse response) {
        OpRequestSend op = pendingRequests.remove(requestId);
        if (op == null) {
            if (log.isDebugEnabled()) {
                log.debug("Got end txn on topic response for timeout {} - {}", response.getTxnidMostBits(),
                        response.getTxnidLeastBits());
            }
            return;
        }

        if (!response.hasError()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Got end txn on topic response for for request {}", op.topic, response.getRequestId());
            }
            log.info("[{}] Got end txn on topic response for for request {}", op.topic, response.getRequestId());
            op.cb.complete(new TxnID(response.getTxnidMostBits(), response.getTxnidLeastBits()));
        } else {
            log.error("[{}] Got end txn on topic response for request {} error {}", op.topic, response.getRequestId(), response.getError());
            op.cb.completeExceptionally(getException(response.getError(), response.getMessage()));
        }
        op.recycle();
    }

    @Override
    public void handleEndTxnOnSubscriptionResponse(long requestId, PulsarApi.CommandEndTxnOnSubscriptionResponse response) {
        OpRequestSend op = pendingRequests.remove(requestId);
        if (op == null) {
            if (log.isDebugEnabled()) {
                log.debug("Got end txn on subscription response for timeout {} - {}", response.getTxnidMostBits(),
                        response.getTxnidLeastBits());
            }
            return;
        }

        if (!response.hasError()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Got end txn on subscription response for for request {}", op.topic, response.getRequestId());
            }
            op.cb.complete(new TxnID(response.getTxnidMostBits(), response.getTxnidLeastBits()));
        } else {
            log.error("[{}] Got end txn on subscription response for request {} error {}", op.topic, response.getRequestId(), response.getError());
            op.cb.completeExceptionally(getException(response.getError(), response.getMessage()));
        }
        op.recycle();
    }

    private CompletableFuture<ClientCnx> cnx(String topic) {
        return getServiceUrl(topic).thenCompose(serviceUrl -> {
            try {
                if (serviceUrl == null) {
                    return CompletableFuture.completedFuture(null);
                }
                URI uri = new URI(serviceUrl);
                return connectionPool.getConnection(InetSocketAddress.createUnresolved(uri.getHost(), uri.getPort())).thenCompose(clientCnx -> {
                    clientCnx.registerTransactionBufferHandler(TransactionBufferHandlerImpl.this);
                    return CompletableFuture.completedFuture(clientCnx);
                });
            } catch (Exception e) {
                return FutureUtil.failedFuture(e);
            }
        });
    }

    private CompletableFuture<String> getServiceUrl(String topic) {
        TopicName topicName = TopicName.get(topic);
        return namespaceService.getBundleAsync(topicName)
                .thenCompose(namespaceService::getOwnerAsync)
                .thenCompose(ned -> {
                    String serviceUrl = null;
                    if (ned.isPresent()) {
                        serviceUrl = ned.get().getNativeUrl();
                    }
                   return CompletableFuture.completedFuture(serviceUrl);
                });
    }

    private TransactionBufferClientException getException(PulsarApi.ServerError serverError, String msg) {
        return new TransactionBufferClientException(msg);
    }

    private boolean canSendRequest(CompletableFuture<?> callback) {
        try {
            if (blockIfReachMaxPendingOps) {
                semaphore.acquire();
            } else {
                if (!semaphore.tryAcquire()) {
                    callback.completeExceptionally(new TransactionBufferClientException("Reach max pending ops."));
                    return false;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            callback.completeExceptionally(TransactionBufferClientException.unwrap(e));
            return false;
        }
        return true;
    }

    @Override
    public void run(Timeout timeout) throws Exception {
        if (timeout.isCancelled()) {
            return;
        }
        long timeToWaitMs;
        OpRequestSend peeked = null;
        Map.Entry<Long, OpRequestSend> firstEntry = pendingRequests.firstEntry();
        peeked = firstEntry == null ? null : firstEntry.getValue();
        while (peeked != null && peeked.createdAt + operationTimeoutInMills - System.currentTimeMillis() <= 0) {
            if (!peeked.cb.isDone()) {
                peeked.cb.completeExceptionally(new TransactionBufferClientException.RequestTimeoutException());
                onResponse(peeked);
            } else {
                break;
            }
            firstEntry = pendingRequests.firstEntry();
            peeked = firstEntry == null ? null : firstEntry.getValue();
        }
        if (peeked == null) {
            timeToWaitMs = operationTimeoutInMills;
        } else {
            long diff = (peeked.createdAt + operationTimeoutInMills) - System.currentTimeMillis();
            if (diff <= 0) {
                timeToWaitMs = operationTimeoutInMills;
            } else {
                timeToWaitMs = diff;
            }
        }
        requestTimeout = timer.newTimeout(this, timeToWaitMs, TimeUnit.MILLISECONDS);
    }

    void onResponse(OpRequestSend op) {
        ReferenceCountUtil.safeRelease(op.byteBuf);
        op.recycle();
        semaphore.release();
    }

    private static final class OpRequestSend {

        long requestId;
        String topic;
        ByteBuf byteBuf;
        CompletableFuture<TxnID> cb;
        long createdAt;

        static OpRequestSend create(long requestId, String topic, ByteBuf byteBuf, CompletableFuture<TxnID> cb) {
            OpRequestSend op = RECYCLER.get();
            op.requestId = requestId;
            op.topic = topic;
            op.byteBuf = byteBuf;
            op.cb = cb;
            op.createdAt = System.nanoTime();
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
}
