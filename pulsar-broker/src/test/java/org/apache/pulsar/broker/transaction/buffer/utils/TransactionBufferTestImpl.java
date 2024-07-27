package org.apache.pulsar.broker.transaction.buffer.utils;

import lombok.Setter;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBuffer;

import java.util.concurrent.CompletableFuture;

public class TransactionBufferTestImpl extends TopicTransactionBuffer {
    @Setter
    public CompletableFuture<Void> transactionBufferFuture = null;
    @Setter
    public State state = null;
    @Setter
    public CompletableFuture<Position> publishFuture = null;

    public TransactionBufferTestImpl(PersistentTopic topic) {
        super(topic);
    }

    @Override
    public CompletableFuture<Void> getTransactionBufferFuture() {
        return transactionBufferFuture == null ? super.getTransactionBufferFuture() : transactionBufferFuture;
    }

    @Override
    public State getState() {
        return state == null ? super.getState() : state;
    }

    @Override
    public CompletableFuture<Position> getPublishFuture() {
       return publishFuture == null ? super.getPublishFuture() : publishFuture;
    }
}
