package org.apache.pulsar.broker.transaction.buffer.utils;

import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferProvider;

public class TransactionBufferTestProvider implements TransactionBufferProvider {

    @Override
    public TransactionBuffer newTransactionBuffer(Topic originTopic) {
        return new TransactionBufferTestImpl((PersistentTopic) originTopic);
    }
}

