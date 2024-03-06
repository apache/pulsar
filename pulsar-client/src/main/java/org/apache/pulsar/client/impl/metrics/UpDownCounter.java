package org.apache.pulsar.client.impl.metrics;

import static org.apache.pulsar.client.impl.metrics.MetricsUtil.getDefaultAggregationLabels;
import static org.apache.pulsar.client.impl.metrics.MetricsUtil.getTopicAttributes;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.LongUpDownCounterBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.extension.incubator.metrics.ExtendedLongCounterBuilder;
import io.opentelemetry.extension.incubator.metrics.ExtendedLongUpDownCounterBuilder;

public class UpDownCounter {

    private final LongUpDownCounter counter;
    private final Attributes attributes;

    UpDownCounter(Meter meter, String name, Unit unit, String description, String topic, Attributes attributes) {
        LongUpDownCounterBuilder builder = meter.upDownCounterBuilder(name)
                .setDescription(description)
                .setUnit(unit.toString());

        if (topic != null) {
            if (builder instanceof ExtendedLongUpDownCounterBuilder) {
                ExtendedLongUpDownCounterBuilder eb = (ExtendedLongUpDownCounterBuilder) builder;
                eb.setAttributesAdvice(getDefaultAggregationLabels(attributes));
            }

            attributes = getTopicAttributes(topic, attributes);
        }

        this.counter = builder.build();
        this.attributes = attributes;
    }

    public void increment() {
        add(1);
    }

    public void decrement() {
        add(-1);
    }

    public void add(long delta) {
        counter.add(delta, attributes);
    }

    public void subtract(long diff) {
        add(-diff);
    }
}
