package org.apache.pulsar.client.impl.metrics;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;

public class Counter {

    private final LongCounter counter;
    private final Attributes attributes;

    Counter(Meter meter, String name, Unit unit, String description, Attributes attributes) {
        counter = meter.counterBuilder(name)
                .setDescription(description)
                .setUnit(unit.toString())
                .build();
        this.attributes = attributes;
    }

    public void increment() {
        add(1);
    }

    public void add(int delta) {
        counter.add(delta, attributes);
    }

}
