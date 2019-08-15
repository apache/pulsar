package org.apache.pulsar.client.api.transaction;

import java.util.concurrent.CompletableFuture;

/**
 * The transaction buffer client to commit or abort transactions on topics.
 */
public interface TransactionBufferClient {

    /**
     * Commit the transaction associated with the topic.
     *
     * @param topic topic name
     * @param txnIdMostBits the most bits of txn id
     * @param txnIdLeastBits the least bits of txn id
     * @return the future represents the commit result
     */
    CompletableFuture<Void> commitTxnOnTopic(String topic, long txnIdMostBits, long txnIdLeastBits);

    /**
     * Abort the transaction associated with the topic.
     *
     * @param topic topic name
     * @param txnIdMostBits the most bits of txn id
     * @param txnIdLeastBits the least bits of txn id
     * @return the future represents the abort result
     */
    CompletableFuture<Void> abortTxnOnTopic(String topic, long txnIdMostBits, long txnIdLeastBits);
}
