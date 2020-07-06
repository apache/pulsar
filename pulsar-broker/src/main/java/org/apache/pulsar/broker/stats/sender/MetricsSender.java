package org.apache.pulsar.broker.stats.sender;

public interface MetricsSender extends AutoCloseable {
    void start();

    void getAndSendMetrics();
}
