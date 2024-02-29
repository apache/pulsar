package org.apache.pulsar.client.impl.metrics;

import com.google.common.collect.Lists;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.Meter;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class LatencyHistogram {

    private static final List<Double> latencyHistogramBuckets =
            Lists.newArrayList(.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0);

    private static final double NANOS = TimeUnit.SECONDS.toNanos(1);


    private final Attributes successAttributes;

    private final Attributes failedAttributes;
    private final DoubleHistogram histogram;

    LatencyHistogram(Meter meter, String name, String description, Attributes attributes) {
        histogram = meter.histogramBuilder(name)
                .setDescription(description)
                .setUnit(Unit.Seconds.toString())
                .setExplicitBucketBoundariesAdvice(latencyHistogramBuckets)
                .build();

        successAttributes = attributes.toBuilder()
                .put("success", true)
                .build();
        failedAttributes = attributes.toBuilder()
                .put("success", false)
                .build();
    }


    public void recordSuccess(long latencyNanos) {
        histogram.record(latencyNanos / NANOS, successAttributes);
    }

    public void recordFailure(long latencyNanos) {
        histogram.record(latencyNanos / NANOS, failedAttributes);
    }
}
