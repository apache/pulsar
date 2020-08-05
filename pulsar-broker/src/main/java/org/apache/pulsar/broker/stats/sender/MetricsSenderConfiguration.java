package org.apache.pulsar.broker.stats.sender;

import org.apache.pulsar.broker.ServiceConfiguration;

public class MetricsSenderConfiguration {
    public String tenant;
    public String namespace;
    public Integer intervalInSeconds;
    public Boolean includeTopicMetrics;
    public Boolean includeConsumerMetrics;

    public MetricsSenderConfiguration(ServiceConfiguration conf) {
        this.tenant = conf.getMetricsSenderDestinationTenant();
        this.namespace = conf.getMetricsSenderDestinationNamespace();
        this.intervalInSeconds = conf.getMetricsSenderIntervalInSeconds();
        this.includeTopicMetrics = conf.getMetricsSenderIncludeTopicMetrics();
        this.includeConsumerMetrics = conf.getMetricsSenderIncludeConsumerMetrics();
    }
}
