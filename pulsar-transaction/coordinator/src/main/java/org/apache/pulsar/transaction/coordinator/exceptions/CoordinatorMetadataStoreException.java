package org.apache.pulsar.transaction.coordinator.exceptions;

public class CoordinatorMetadataStoreException extends CoordinatorException{
    public CoordinatorMetadataStoreException(String message) {
        super(message);
    }

    public CoordinatorMetadataStoreException(Throwable cause) {
        super(cause);
    }

    public CoordinatorMetadataStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
