package org.apache.pulsar.transaction.common.exception;

public class TransactionAbortConflictException extends TransactionConflictException {

    private static final long serialVersionUID = 0L;

    public TransactionAbortConflictException(String message) {
        super(message);
    }
}
