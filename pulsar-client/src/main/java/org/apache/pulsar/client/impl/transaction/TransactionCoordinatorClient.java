package org.apache.pulsar.client.impl.transaction;

import java.util.concurrent.CompletableFuture;

/**
 * The transaction coordinator client to commit and abort transactions on topics.
 */
public interface TransactionCoordinatorClient {

    /**
     * Commit the transaction associated with the topic.
     *
     * @param topic topic name
     * @param txnIdMostBits the most bits of txn id
     * @param txnIdLeastBits the least bits of txn id
     * @return the future represents the commit result
     */
    CompletableFuture<Void> commitTxnOnTopic(String topic,
                                             long txnIdMostBits,
                                             long txnIdLeastBits);

    /**
     * Abort the transaction associated with the topic.
     *
     * @param topic topic name
     * @param txnIdMostBits the most bits of txn id
     * @param txnIdLeastBits the least bits of txn id
     * @return the future represents the abort result
     */
    CompletableFuture<Void> abortTxnOnTopic(String topic,
                                            long txnIdMostBits,
                                            long txnIdLeastBits);

    /**
     * Commit the transaction associated with the topic subscription.
     *
     * @param topic topic name
     * @param subscription subscription name
     * @param txnIdMostBits the most bits of txn id
     * @param txnIdLeastBits the least bits of txn id
     * @return the future represents the commit result
     */
    CompletableFuture<Void> commitTxnOnSubscription(String topic,
                                                    String subscription,
                                                    long txnIdMostBits,
                                                    long txnIdLeastBits);

    /**
     * Abort the transaction associated with the topic subscription.
     *
     * @param topic topic name
     * @param subscription subscription name
     * @param txnIdMostBits the most bits of txn id
     * @param txnIdLeastBits the least bits of txn id
     * @return the future represents the abort result
     */
    CompletableFuture<Void> abortTxnOnSubscription(String topic,
                                                   String subscription,
                                                   long txnIdMostBits,
                                                   long txnIdLeastBits);

}
