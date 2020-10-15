package org.apache.pulsar.client.impl.transaction;

import org.apache.pulsar.client.api.transaction.TransactionResult;
import org.apache.pulsar.client.impl.MessageIdImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * This class represents the result of transaction commit request.
 */
public class TransactionEndResult implements TransactionResult {

    private Map<String, MessageIdImpl> messageIdMap = new HashMap<>();

    public void putMessageId(String partition, MessageIdImpl messageId) {
        messageIdMap.putIfAbsent(partition, messageId);
    }

    public MessageIdImpl getMessageId(String partition) {
        return messageIdMap.get(partition);
    }

}
