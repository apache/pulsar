package org.apache.pulsar.client.impl;

import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.impl.ConsumerImpl.RequestTime;

@Slf4j
public class AckTimeoutTracker<T> implements TimerTask {

    private final ConcurrentLongHashMap<OpForAckCallBack> ackRequests;
    private final ConcurrentLinkedQueue<RequestTime> ackTimeoutQueue;
    private final ConsumerImpl<T> consumer;
    private final long ackResponseTimeout;
    private volatile Timeout timeout;

    public AckTimeoutTracker(ConsumerImpl<T> consumer) {
        this.ackRequests = new ConcurrentLongHashMap<>(16, 1);
        this.ackTimeoutQueue = new ConcurrentLinkedQueue<>();
        this.consumer = consumer;
        this.ackResponseTimeout = consumer.conf.getAckResponseTimeout();
        this.timeout = consumer.getClient().timer().newTimeout(this, ackResponseTimeout, TimeUnit.MILLISECONDS);
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
        RequestTime peeked = ackTimeoutQueue.peek();
        while (peeked != null && peeked.creationTimeMs + ackResponseTimeout
                - System.currentTimeMillis() <= 0) {
            RequestTime lastPolled = ackTimeoutQueue.poll();
            if (lastPolled != null) {
                OpForAckCallBack op = ackRequests.remove(lastPolled.requestId);
                if (op != null && !op.callback.isDone()) {
                    StringBuilder stringBuilder = new StringBuilder();
                    stringBuilder.append("MessageId : [")
                            .append(op.messageId)
                            .append("]Could not get response from broker within given timeout!");
                    if (op.txnID.getLeastSigBits() != -1 && op.txnID.getMostSigBits() != -1) {
                        stringBuilder.append(" TxnId : ")
                                .append(op.txnID);
                    }
                    op.callback.completeExceptionally(new PulsarClientException
                            .TimeoutException(stringBuilder.toString()));
                    if (log.isDebugEnabled()) {
                        log.debug("MessageId : [{}] Ack RequestId : {} TxnId : [{}] is timeout.",
                                op.messageId, lastPolled.requestId, op.txnID);
                    }
                    onTransactionAckResponse(op);
                }
            } else {
                break;
            }
            peeked = ackTimeoutQueue.peek();
        }

        if (peeked == null) {
            timeToWaitMs = ackResponseTimeout;
        } else {
            long diff = (peeked.creationTimeMs + ackResponseTimeout) - System.currentTimeMillis();
            if (diff <= 0) {
                timeToWaitMs = ackResponseTimeout;
            } else {
                timeToWaitMs = diff;
            }
        }
        this.timeout = consumer.getClient().timer().newTimeout(this, timeToWaitMs, TimeUnit.MILLISECONDS);
    }

    public void cancel() {
        this.timeout.cancel();
    }

    public void add(OpForAckCallBack op, long requestId) {
        ackRequests.put(requestId, op);
        ackTimeoutQueue.add(new RequestTime(System.currentTimeMillis(), requestId));
    }

    public OpForAckCallBack remove(long requestId) {
        return ackRequests.remove(requestId);
    }

    private void onTransactionAckResponse(OpForAckCallBack op) {
        ReferenceCountUtil.safeRelease(op.cmd);
        op.recycle();
    }

    protected static class OpForAckCallBack {
        protected ByteBuf cmd;
        protected CompletableFuture<Void> callback;
        protected MessageIdImpl messageId;
        protected TxnID txnID;

        static OpForAckCallBack create(ByteBuf cmd, CompletableFuture<Void> callback, MessageId messageId, TxnID txnID) {
            OpForAckCallBack op = RECYCLER.get();
            op.callback = callback;
            op.cmd = cmd;
            op.messageId = (MessageIdImpl) messageId;
            op.txnID = txnID;
            return op;
        }
        private OpForAckCallBack(Recycler.Handle<OpForAckCallBack> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        void recycle() {
            recyclerHandle.recycle(this);
        }

        private final Recycler.Handle<OpForAckCallBack> recyclerHandle;
        private static final Recycler<OpForAckCallBack> RECYCLER = new Recycler<OpForAckCallBack>() {
            @Override
            protected OpForAckCallBack newObject(Handle<OpForAckCallBack> handle) {
                return new OpForAckCallBack(handle);
            }
        };
    }
}
