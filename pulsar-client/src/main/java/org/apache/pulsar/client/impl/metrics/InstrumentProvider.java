package org.apache.pulsar.client.impl.metrics;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import org.apache.pulsar.client.api.MetricsCardinality;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.naming.TopicName;

public class InstrumentProvider {

    private final Meter meter;
    private final MetricsCardinality metricsCardinality;

    public InstrumentProvider(ClientConfigurationData conf) {
        this.meter = conf.getOpenTelemetry().getMeter("org.apache.pulsar.client");
        this.metricsCardinality = conf.getOpenTelemetryMetricsCardinality();
    }

    public Attributes getAttributes(String topic) {
        if (metricsCardinality == MetricsCardinality.None) {
            return Attributes.empty();
        }

        AttributesBuilder ab = Attributes.builder();
        TopicName tn = TopicName.get(topic);

        switch (metricsCardinality) {
            case Partition:
                if (tn.isPartitioned()) {
                    ab.put("partition", tn.getPartitionIndex());
                }
                // fallthrough
            case Topic:
                ab.put("topic", tn.getPartitionedTopicName());
                // fallthrough
            case Namespace:
                ab.put("namespace", tn.getNamespace());
                // fallthrough
            case Tenant:
                ab.put("tenant", tn.getTenant());
        }

        return ab.build();
    }

    public Counter newCounter(String name, Unit unit, String description, Attributes attributes) {
        return new Counter(meter, name, unit, description, attributes);
    }

    public UpDownCounter newUpDownCounter(String name, Unit unit, String description, Attributes attributes) {
        return new UpDownCounter(meter, name, unit, description, attributes);
    }

    public LatencyHistogram newLatencyHistogram(String name, String description, Attributes attributes) {
        return new LatencyHistogram(meter, name, description, attributes);
    }
}
