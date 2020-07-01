package org.apache.pulsar.broker.stats.sender;

public interface MetricsSender {
    void start();

    void getAndSendMetrics();
}
