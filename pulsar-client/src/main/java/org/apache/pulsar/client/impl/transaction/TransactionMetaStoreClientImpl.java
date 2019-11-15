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

import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.transaction.TransactionMetaStoreClient;
import org.apache.pulsar.client.api.transaction.TransactionMetaStoreClientException;
import org.apache.pulsar.client.api.transaction.TransactionMetaStoreClientException.TransactionMetaStoreClientStateException;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.PartitionedProducerImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.TransactionMetaStoreHandler;
import org.apache.pulsar.client.util.MathUtils;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.GrowableArrayBlockingQueue;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Transaction meta store client based topic assigned.
 */
public class TransactionMetaStoreClientImpl implements TransactionMetaStoreClient, TransactionMetaStoreRequestHandler, TimerTask {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionMetaStoreClientImpl.class);

    private final PulsarClientImpl pulsarClient;
    private Producer<byte[]> producerForAssignTopic = null;
    private TransactionMetaStoreHandler[] handlers;
    private final AtomicLong epoch = new AtomicLong(0);
    private volatile Timeout requestTimeout = null;
    private final boolean blockIfReachMaxPendingOps;

    private final Semaphore semaphore;
    private final BlockingQueue<OpNewTransaction> pendingNewTransactionOps;

    private static final AtomicReferenceFieldUpdater<TransactionMetaStoreClientImpl, State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(TransactionMetaStoreClientImpl.class, State.class, "state");
    private volatile State state = State.NONE;

    public TransactionMetaStoreClientImpl(PulsarClient pulsarClient) {
        this.pulsarClient = (PulsarClientImpl) pulsarClient;
        this.pendingNewTransactionOps = new GrowableArrayBlockingQueue<>();
        this.semaphore = new Semaphore(DEFAULT_MAX_PADDING_OPS);
        this.blockIfReachMaxPendingOps = BLOCK_IF_REACH_MAX_PADDING_OPS;
    }

    @Override
    public void run(Timeout timeout) throws Exception {
        if (timeout.isCancelled()) {
            return;
        }
        long timeToWaitMs;
        synchronized (this) {
        }
    }

    @Override
    public void start() throws TransactionMetaStoreClientException {
        try {
            startAsync().get();
        } catch (Exception e) {
            throw TransactionMetaStoreClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<Void> startAsync() {
        if (STATE_UPDATER.compareAndSet(this, State.NONE, State.STARTING)) {
            return pulsarClient.newProducer(Schema.BYTES)
                .topic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString())
                .createAsync().thenApply(p -> {
                    producerForAssignTopic = p;
                    if (p instanceof PartitionedProducerImpl) {
                        List<ProducerImpl<byte[]>> producerList = ((PartitionedProducerImpl<byte[]>) p).getProducers();
                        handlers = new TransactionMetaStoreHandler[producerList.size()];
                        for (ProducerImpl<byte[]> producer : producerList) {
                            producer.cnx().setTransactionMetaStoreRequestHandler(TransactionMetaStoreClientImpl.this);
                            cnxs[TopicName.get(producer.getTopic()).getPartitionIndex()] = producer.cnx();
                        }
                    } else {
                        cnxs[0] = ((ProducerImpl) p).cnx();
                    }
                    return null;
                });
        } else {
            return FutureUtil.failedFuture(new TransactionMetaStoreClientStateException("Can not start while current state is " + state));
        }
    }

    @Override
    public void close() throws TransactionMetaStoreClientException {
        try {
            closeAsync().get();
        } catch (Exception e) {
            throw TransactionMetaStoreClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> result = new CompletableFuture<>();
        if (getState() == State.CLOSING || getState() == State.CLOSED) {
            LOG.warn("The transaction meta store is closing or closed, doing nothing.");
            result.complete(null);
        } else {
            this.cnxs = null;
            if (producerForAssignTopic != null) {
                producerForAssignTopic.closeAsync().whenComplete((v, ex) -> {
                    if (ex != null) {
                        result.completeExceptionally(ex);
                    } else {
                        result.complete(null);
                    }
                });
            } else {
                result.complete(null);
            }
        }
        return result;
    }

    @Override
    public TxnID newTransaction() throws TransactionMetaStoreClientException {
        try {
            return newTransactionAsync().get();
        } catch (Exception e) {
            throw TransactionMetaStoreClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<TxnID> newTransactionAsync() {
        return newTransactionAsync(DEFAULT_TXN_TTL_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public TxnID newTransaction(long timeout, TimeUnit unit) throws TransactionMetaStoreClientException {
        try {
            return newTransactionAsync(timeout, unit).get();
        } catch (Exception e) {
            throw TransactionMetaStoreClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<TxnID> newTransactionAsync(long timeout, TimeUnit unit) {
        CompletableFuture<TxnID> callback = new CompletableFuture<>();

        if (!canSendRequest(callback)) {
            return callback;
        }

        ByteBuf cmd = Commands.newTxn(pulsarClient.newRequestId(), unit.toMillis(timeout));
        OpNewTransaction op = OpNewTransaction.create(cmd, callback);
        pendingNewTransactionOps.add(op);
        cmd.retain();
        return callback;
    }



    @Override
    public State getState() {
        return state;
    }

    private TransactionMetaStoreHandler nextHandler() {
        int index = MathUtils.signSafeMod(epoch.incrementAndGet(), handlers.length);
        return handlers[index];
    }

    private static class OpNewTransaction {
        long createdAt;
        ByteBuf cmd;
        CompletableFuture<TxnID> callback;

        static OpNewTransaction create(ByteBuf cmd, CompletableFuture<TxnID> callback) {
            OpNewTransaction op = RECYCLER.get();
            op.callback = callback;
            op.createdAt = System.currentTimeMillis();
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
}
