package org.apache.pulsar.transaction.common.exception;

public class TransactionCommitConflictException extends TransactionConflictException {

    private static final long serialVersionUID = 0L;

    public TransactionCommitConflictException(String message) {
        super(message);
    }
}
