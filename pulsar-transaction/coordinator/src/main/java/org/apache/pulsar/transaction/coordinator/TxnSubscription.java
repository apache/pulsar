package org.apache.pulsar.transaction.coordinator;

public class TxnSubscription {
    private String topic;
    private String subscription;

    public TxnSubscription(String topic, String subscription) {
        this.topic = topic;
        this.subscription = subscription;
    }

    @Override
    public int hashCode() {
        return (topic + subscription).hashCode();
    }
}
