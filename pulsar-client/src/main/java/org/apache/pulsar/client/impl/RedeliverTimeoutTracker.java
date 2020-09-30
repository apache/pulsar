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
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.TxnID;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.impl.ConsumerImpl.RequestTime;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;

@Slf4j
public class RedeliverTimeoutTracker<T> implements TimerTask {

    private final ConcurrentLongHashMap<OpForRedeliverCallBack> redeliverRequests;
    private final ConcurrentLinkedQueue<RequestTime> redeliverTimeoutQueue;
    private final ConsumerImpl<T> consumer;
    private final long redeliverTimeout;
    private volatile Timeout timeout;


    public RedeliverTimeoutTracker(ConsumerImpl<T> consumer) {
        this.redeliverRequests = new ConcurrentLongHashMap<>();
        this.redeliverTimeoutQueue = new ConcurrentLinkedQueue<>();
        this.consumer = consumer;
        this.redeliverTimeout = consumer.conf.getRedeliverTimeout();
        this.timeout = consumer.getClient().timer().newTimeout(this, redeliverTimeout, TimeUnit.MILLISECONDS);
    }

    public void add(OpForRedeliverCallBack opForRedeliverCallBack, long requestId) {
        this.redeliverRequests.put(requestId, opForRedeliverCallBack);
        this.redeliverTimeoutQueue.add(new RequestTime(System.currentTimeMillis(), requestId));
    }

    public void cancel() {
        this.timeout.cancel();
    }

    public OpForRedeliverCallBack remove(long requestId) {
        return redeliverRequests.remove(requestId);
    }

    @Override
    public void run(Timeout timeout) throws Exception {
        if (timeout.isCancelled()) {
            return;
        }
        long timeToWaitMs;
        if (consumer.getState() == HandlerState.State.Closing || consumer.getState() == HandlerState.State.Closed) {
            return;
        }
        RequestTime peeked = redeliverTimeoutQueue.peek();
        while (peeked != null && peeked.creationTimeMs + redeliverTimeout - System.currentTimeMillis() <= 0) {
            RequestTime lastPolled = redeliverTimeoutQueue.poll();
            if (lastPolled != null) {
                OpForRedeliverCallBack op = redeliverRequests.remove(lastPolled.requestId);
                if (op != null && !op.callback.isDone()) {
                    op.callback.completeExceptionally(new PulsarClientException.TimeoutException(
                            "Could not get response from broker within given timeout."));
                    if (log.isDebugEnabled()) {
                        log.debug("Redeliver timeout. requestId : [{}]", lastPolled.requestId);
                    }
                    onRedeliver(op);
                }
            } else {
                break;
            }
            peeked = redeliverTimeoutQueue.peek();
        }

        if (peeked == null) {
            timeToWaitMs = redeliverTimeout;
        } else {
            long diff = (peeked.creationTimeMs + redeliverTimeout) - System.currentTimeMillis();
            if (diff <= 0) {
                timeToWaitMs = redeliverTimeout;
            } else {
                timeToWaitMs = diff;
            }
        }
        this.timeout = consumer.getClient().timer().newTimeout(this, timeToWaitMs, TimeUnit.MILLISECONDS);
    }

    protected static class OpForRedeliverCallBack {
        protected ByteBuf cmd;
        protected CompletableFuture<Void> callback;
        protected TxnID txnID;
        protected long requestId;

        static OpForRedeliverCallBack create(ByteBuf cmd, TxnID txnID, CompletableFuture<Void> callback, long requestId) {
            OpForRedeliverCallBack op = RECYCLER.get();
            op.callback = callback;
            op.cmd = cmd;
            op.requestId = requestId;
            op.txnID = txnID;
            return op;
        }
        private OpForRedeliverCallBack(Recycler.Handle<OpForRedeliverCallBack> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        void recycle() {
            recyclerHandle.recycle(this);
        }

        private final Recycler.Handle<OpForRedeliverCallBack> recyclerHandle;
        private static final Recycler<OpForRedeliverCallBack> RECYCLER = new Recycler<OpForRedeliverCallBack>() {
            @Override
            protected OpForRedeliverCallBack newObject(Handle<OpForRedeliverCallBack> handle) {
                return new OpForRedeliverCallBack(handle);
            }
        };
    }

    private void onRedeliver(OpForRedeliverCallBack op) {
        ReferenceCountUtil.safeRelease(op.cmd);
        op.recycle();
    }
}
