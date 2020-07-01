package org.apache.pulsar.broker.stats.sender;

import org.apache.pulsar.broker.ServiceConfiguration;

public class MetricsSenderConfiguration {
    public String tenant;
    public Integer intervalInSeconds;

    public MetricsSenderConfiguration(ServiceConfiguration conf) {
        this.tenant = conf.getMetricsSenderDestinationTenant();
        this.intervalInSeconds = conf.getMetricsSenderIntervalInSeconds();
    }
}
