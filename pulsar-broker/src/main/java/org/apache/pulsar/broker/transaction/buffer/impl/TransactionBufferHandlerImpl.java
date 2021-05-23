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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.netty.buffer.ByteBuf;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.TransactionBufferClientException;
import org.apache.pulsar.client.api.transaction.TransactionBufferClientException.ReachMaxPendingOpsException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.transaction.TransactionBufferHandler;
import org.apache.pulsar.common.api.proto.CommandEndTxnOnPartitionResponse;
import org.apache.pulsar.common.api.proto.CommandEndTxnOnSubscriptionResponse;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.common.protocol.Commands;

@Slf4j
public class TransactionBufferHandlerImpl implements TransactionBufferHandler, TimerTask {

    private final ConcurrentSkipListMap<Long, OpRequestSend> pendingRequests;
    private final AtomicLong requestIdGenerator = new AtomicLong();
    private final long operationTimeoutInMills;
    private Timeout requestTimeout;
    private final HashedWheelTimer timer;
    private final Semaphore semaphore;
    private final boolean blockIfReachMaxPendingOps;
    private final PulsarClient pulsarClient;

    private final LoadingCache<String, CompletableFuture<ClientCnx>> cache = CacheBuilder.newBuilder()
            .maximumSize(100000)
            .expireAfterAccess(30, TimeUnit.MINUTES)
            .build(new CacheLoader<String, CompletableFuture<ClientCnx>>() {
                @Override
                public CompletableFuture<ClientCnx> load(String topic) {
                    CompletableFuture<ClientCnx> siFuture = getClientCnx(topic);
                    siFuture.whenComplete((si, cause) -> {
                        if (null != cause) {
                            cache.invalidate(topic);
                        }
                    });
                    return siFuture;
                }
            });

    public TransactionBufferHandlerImpl(PulsarClient pulsarClient,
                                        HashedWheelTimer timer) {
        this.pulsarClient = pulsarClient;
        this.pendingRequests = new ConcurrentSkipListMap<>();
        this.operationTimeoutInMills = 3000L;
        this.semaphore = new Semaphore(10000);
        this.blockIfReachMaxPendingOps = true;
        this.timer = timer;
        this.requestTimeout = timer.newTimeout(this, operationTimeoutInMills, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized CompletableFuture<TxnID> endTxnOnTopic(String topic, long txnIdMostBits, long txnIdLeastBits,
                                                  TxnAction action, long lowWaterMark) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] endTxnOnTopic txnId: [{}], txnAction: [{}]",
                    topic, new TxnID(txnIdMostBits, txnIdLeastBits), action.getValue());
        }
        CompletableFuture<TxnID> cb = new CompletableFuture<>();
        if (!canSendRequest(cb)) {
            return cb;
        }
        long requestId = requestIdGenerator.getAndIncrement();
        ByteBuf cmd = Commands.newEndTxnOnPartition(requestId, txnIdLeastBits, txnIdMostBits,
                topic, action, lowWaterMark);
        return endTxn(requestId, topic, cmd, cb);
    }

    @Override
    public synchronized CompletableFuture<TxnID> endTxnOnSubscription(String topic, String subscription,
                                                                      long txnIdMostBits, long txnIdLeastBits,
                                                                      TxnAction action, long lowWaterMark) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] endTxnOnSubscription txnId: [{}], txnAction: [{}]",
                    topic, new TxnID(txnIdMostBits, txnIdLeastBits), action.getValue());
        }
        CompletableFuture<TxnID> cb = new CompletableFuture<>();
        if (!canSendRequest(cb)) {
            return cb;
        }
        long requestId = requestIdGenerator.getAndIncrement();
        ByteBuf cmd = Commands.newEndTxnOnSubscription(requestId, txnIdLeastBits, txnIdMostBits,
                topic, subscription, action, lowWaterMark);
        return endTxn(requestId, topic, cmd, cb);
    }

    private CompletableFuture<TxnID> endTxn(long requestId, String topic, ByteBuf cmd, CompletableFuture<TxnID> cb) {
        OpRequestSend op = OpRequestSend.create(requestId, topic, cmd, cb);
        try {
            cache.get(topic).whenComplete((clientCnx, throwable) -> {
                if (throwable == null) {
                    if (clientCnx.ctx().channel().isActive()) {
                        clientCnx.registerTransactionBufferHandler(TransactionBufferHandlerImpl.this);
                        synchronized (TransactionBufferHandlerImpl.this) {
                            pendingRequests.put(requestId, op);
                            cmd.retain();
                        }
                        clientCnx.ctx().writeAndFlush(cmd, clientCnx.ctx().voidPromise());
                    } else {
                        cache.invalidate(topic);
                        cb.completeExceptionally(
                                new PulsarClientException.LookupException(topic + " endTxn channel is not active"));
                        op.recycle();
                    }
                } else {
                    log.error("endTxn error topic: [{}]", topic, throwable);
                    cache.invalidate(topic);
                    cb.completeExceptionally(
                            new PulsarClientException.LookupException(throwable.getMessage()));
                    op.recycle();
                }
            });
        } catch (ExecutionException e) {
            log.error("endTxn channel is not active exception", e);
            cache.invalidate(topic);
            cb.completeExceptionally(new PulsarClientException.LookupException(e.getCause().getMessage()));
            op.recycle();
        }
        return cb;
    }

    @Override
    public synchronized void handleEndTxnOnTopicResponse(long requestId, CommandEndTxnOnPartitionResponse response) {
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
            op.cb.complete(new TxnID(response.getTxnidMostBits(), response.getTxnidLeastBits()));
        } else {
            log.error("[{}] Got end txn on topic response for request {} error {}", op.topic, response.getRequestId(),
                    response.getError());
            cache.invalidate(op.topic);
            op.cb.completeExceptionally(ClientCnx.getPulsarClientException(response.getError(), response.getMessage()));
        }
        onResponse(op);
    }

    @Override
    public synchronized void handleEndTxnOnSubscriptionResponse(long requestId,
                                                   CommandEndTxnOnSubscriptionResponse response) {
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
                log.debug("[{}] Got end txn on subscription response for for request {}",
                        op.topic, response.getRequestId());
            }
            op.cb.complete(new TxnID(response.getTxnidMostBits(), response.getTxnidLeastBits()));
        } else {
            log.error("[{}] Got end txn on subscription response for request {} error {}",
                    op.topic, response.getRequestId(), response.getError());
            cache.invalidate(op.topic);
            op.cb.completeExceptionally(ClientCnx.getPulsarClientException(response.getError(), response.getMessage()));
        }
        onResponse(op);
    }

    private boolean canSendRequest(CompletableFuture<?> callback) {
        try {
            if (blockIfReachMaxPendingOps) {
                semaphore.acquire();
            } else {
                if (!semaphore.tryAcquire()) {
                    callback.completeExceptionally(new ReachMaxPendingOpsException("Reach max pending ops."));
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

    public synchronized void run(Timeout timeout) throws Exception {
        if (timeout.isCancelled()) {
            return;
        }
        long timeToWaitMs;
        OpRequestSend peeked;
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
            pendingRequests.remove(pendingRequests.firstKey());
            peeked = firstEntry == null ? null : firstEntry.getValue();
        }
        if (peeked == null) {
            timeToWaitMs = operationTimeoutInMills;
        } else {
            timeToWaitMs = (peeked.createdAt + operationTimeoutInMills) - System.currentTimeMillis();
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
            op.createdAt = System.currentTimeMillis();
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

    private CompletableFuture<ClientCnx> getClientCnx(String topic) {
        return ((PulsarClientImpl) pulsarClient).getConnection(topic);
    }

    @Override
    public void close() {
        this.timer.stop();
    }
}
