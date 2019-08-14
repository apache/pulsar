package org.apache.pulsar.client.impl.transaction;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * The implementation of {@link TransactionCoordinatorClient}.
 */
public class TransactionCoordinatorClientImpl implements TransactionCoordinatorClient {

    private final PulsarClientImpl client;

    public TransactionCoordinatorClientImpl(PulsarClientImpl client) {
        this.client = client;
    }

    @Override
    public CompletableFuture<Void> commitTxnOnTopic(String topic, long txnIdMostBits, long txnIdLeastBits) {
        return FutureUtil.failedFuture(
            new UnsupportedOperationException("Not Implemented Yet"));
    }

    @Override
    public CompletableFuture<Void> abortTxnOnTopic(String topic, long txnIdMostBits, long txnIdLeastBits) {
        return FutureUtil.failedFuture(
            new UnsupportedOperationException("Not Implemented Yet"));
    }

    @Override
    public CompletableFuture<Void> commitTxnOnSubscription(String topic, String subscription, long txnIdMostBits, long txnIdLeastBits) {
        return FutureUtil.failedFuture(
            new UnsupportedOperationException("Not Implemented Yet"));
    }

    @Override
    public CompletableFuture<Void> abortTxnOnSubscription(String topic, String subscription, long txnIdMostBits, long txnIdLeastBits) {
        return FutureUtil.failedFuture(
            new UnsupportedOperationException("Not Implemented Yet"));
    }
}
