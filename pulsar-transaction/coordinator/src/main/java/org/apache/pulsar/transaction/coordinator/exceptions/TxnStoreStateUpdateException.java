package org.apache.pulsar.transaction.coordinator.exceptions;

import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;

/**
 * Exception is thrown when update the state incorrect in transaction store.
 */
public class TxnStoreStateUpdateException extends CoordinatorException {

    private static final long serialVersionUID = 0L;

    public TxnStoreStateUpdateException(String message) {
        super(message);
    }

    public TxnStoreStateUpdateException(String tcID,
                                        TransactionMetadataStore.State oldState,
                                        TransactionMetadataStore.State newState) {
        super(
                "Expect tcID `" + tcID + "` to be in " + oldState
                        + " status but it is in " + newState + " status");

    }
}
