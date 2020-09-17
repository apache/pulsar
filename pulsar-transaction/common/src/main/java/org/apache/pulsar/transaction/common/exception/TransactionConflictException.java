package org.apache.pulsar.transaction.common.exception;

public class TransactionConflictException extends Exception{

    private static final long serialVersionUID = 0L;

    public TransactionConflictException(String message) {
        super(message);
    }
}
