package org.apache.pulsar.client.impl.metrics;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.Meter;

public class UpDownCounter {

    private final LongUpDownCounter counter;
    private final Attributes attributes;

    UpDownCounter(Meter meter, String name, Unit unit, String description, Attributes attributes) {
        counter = meter.upDownCounterBuilder(name)
                .setDescription(description)
                .setUnit(unit.toString())
                .build();
        this.attributes = attributes;
    }

    public void increment() {
        add(1);
    }

    public void decrement() {
        add(-1);
    }

    public void add(int delta) {
        counter.add(delta, attributes);
    }

    public void subtract(int diff) {
        add(-diff);
    }
}
