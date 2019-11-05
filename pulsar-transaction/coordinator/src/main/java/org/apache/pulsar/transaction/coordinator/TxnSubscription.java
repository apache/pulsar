package org.apache.pulsar.transaction.coordinator;

import lombok.Data;

/**
 * An class represents the subscription of a transaction in {@link TxnSubscription}.
 */
@Data
public class TxnSubscription {
    private String topic;
    private String subscription;

    public TxnSubscription(String topic, String subscription) {
        this.topic = topic;
        this.subscription = subscription;
    }
}
