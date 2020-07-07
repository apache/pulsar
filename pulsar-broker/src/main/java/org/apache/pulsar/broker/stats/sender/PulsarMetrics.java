package org.apache.pulsar.broker.stats.sender;

public class PulsarMetrics {
    public String head;
    public String body;

    @Override
    public String toString() {
        return "PulsarMetrics{" +
                "head='" + head + '\'' +
                ", body='" + body + '\'' +
                '}';
    }

    public PulsarMetrics() {
        super();
    }

    public PulsarMetrics(String head, String body) {
        this.head = head;
        this.body = body;
    }
}
