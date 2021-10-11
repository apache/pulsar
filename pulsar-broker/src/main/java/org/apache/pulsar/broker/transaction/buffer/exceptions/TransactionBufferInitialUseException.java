package org.apache.pulsar.broker.transaction.buffer.exceptions;

public class TransactionBufferInitialUseException extends TransactionBufferException{

    private static final long serialVersionUID = 0L;

    public TransactionBufferInitialUseException(String message) {
        super(message);
    }
}
