package org.apache.pulsar.transaction.common.exception;

/**
 * Exception thrown when a transaction try to acknowledge message when it shouldn't.
 *
 */
public class TransactionConflictException extends Exception {

    private static final long serialVersionUID = 0L;

    public TransactionConflictException(String message) {
        super(message);
    }
}