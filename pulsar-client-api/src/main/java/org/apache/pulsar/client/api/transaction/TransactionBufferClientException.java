package org.apache.pulsar.client.api.transaction;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Exceptions for transaction buffer client.
 */
public class TransactionBufferClientException extends IOException {

    public TransactionBufferClientException(Throwable t) {
        super(t);
    }

    public TransactionBufferClientException(String message) {
        super(message);
    }

    /**
     * Thrown when operation timeout.
     */
    public static class RequestTimeoutException extends TransactionBufferClientException {

        public RequestTimeoutException() {
            super("Transaction buffer request timeout.");
        }

        public RequestTimeoutException(String message) {
            super(message);
        }
    }

    public static TransactionBufferClientException unwrap(Throwable t) {
        if (t instanceof TransactionBufferClientException) {
            return (TransactionBufferClientException) t;
        } else if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        } else if (t instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            return new TransactionBufferClientException(t);
        }  else if (!(t instanceof ExecutionException)) {
            // Generic exception
            return new TransactionBufferClientException(t);
        }

        Throwable cause = t.getCause();
        String msg = cause.getMessage();

        if (cause instanceof TransactionBufferClientException.RequestTimeoutException) {
            return new TransactionBufferClientException.RequestTimeoutException(msg);
        } else {
            return new TransactionBufferClientException(t);
        }

    }
}
