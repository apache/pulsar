package org.apache.pulsar.broker.transaction.buffer;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBufferRecoverCallBack;


public interface AbortedTxnProcessor<T> {

    /**
     * After the transaction buffer writes a transaction aborted mark to the topic,
     * the transaction buffer will add the aborted transaction ID to AbortedTxnProcessor.
     * @param txnID aborted transaction ID.
     */
    void appendAbortedTxn(T txnID, PositionImpl position);

    /**
     * After the transaction buffer writes a transaction aborted mark to the topic,
     * the transaction buffer will update max read position in AbortedTxnProcessor
     * @param maxReadPosition  the Max read position after the transaction is aborted.
     */
    void updateMaxReadPosition(Position maxReadPosition);


    /**
     * Pulsar has a configuration for ledger retention time.
     * If the transaction aborted mark position has been deleted, the transaction is valid and can be clear.
     * In the old implementation we clear the invalid aborted txn ID one by one.
     * In the new implementation, we adopt snapshot segments. And then we clear invalid segment by its max read position.
     */
    void trimExpiredTxnIDDataOrSnapshotSegments();

    /**
     * Check whether the transaction ID is an aborted transaction ID.
     * @param txnID the transaction ID that needs to be checked.
     * @param readPosition the read position of the transaction message, can be used to find the segment.
     * @return a boolean, whether the transaction ID is an aborted transaction ID.
     */
    boolean checkAbortedTransaction(T  txnID, Position readPosition);

    /**
     * Recover transaction buffer by transaction buffer snapshot.
     * @return a pair consists of a Boolean if the transaction buffer needs to recover and a Position (startReadCursorPosition) determiner where to start to recover in the original topic.
     */

    CompletableFuture<Object> recoverFromSnapshot(TopicTransactionBufferRecoverCallBack callBack);

}