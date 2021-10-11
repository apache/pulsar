package org.apache.pulsar.broker.transaction.buffer.exceptions;


import org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBufferState;

public class TransactionBufferStatusException extends TransactionBufferException{

    private static final long serialVersionUID = 0L;

    public TransactionBufferStatusException(String topic,
                                            TopicTransactionBufferState.State expectState,
                                            TopicTransactionBufferState.State state) {
        super("TransactionBuffer of " + topic + " is not in a expect status `" + expectState + "`, but is in `" + state
                + "` status ");
    }
}
