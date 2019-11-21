package org.apache.pulsar.transaction.coordinator;

import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.pulsar.common.api.proto.PulsarApi;

/**
 * A log interface for transaction to read and write transaction operation.
 */
public interface TransactionLog {

    /**
     * Read the entry from bookkeeper.
     *
     * @param numberOfEntriesToRead the number of reading entry
     * @param callback the callback to executing when reading entry async finished
     */
    void read(int numberOfEntriesToRead, AsyncCallbacks.ReadEntriesCallback callback, Object ctx);

    /**
     * Close the transaction log.
     */
    CompletableFuture<Void> close();

    /**
     * Write the transaction operation to the transaction log.
     *
     * @param transactionMetadataEntry {@link PulsarApi.TransactionMetadataEntry} transaction metadata entry
     * @return a future represents the result of this operation
     */
    CompletableFuture<Void> write(PulsarApi.TransactionMetadataEntry transactionMetadataEntry);
}
