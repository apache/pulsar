/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pulsar.client.impl.metrics;

import static org.apache.pulsar.client.impl.metrics.MetricsUtil.getDefaultAggregationLabels;
import static org.apache.pulsar.client.impl.metrics.MetricsUtil.getTopicAttributes;
import com.google.common.collect.Lists;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.DoubleHistogramBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.extension.incubator.metrics.ExtendedDoubleHistogramBuilder;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class LatencyHistogram {

    private static final List<Double> latencyHistogramBuckets =
            Lists.newArrayList(.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0);

    private static final double NANOS = TimeUnit.SECONDS.toNanos(1);


    private final Attributes successAttributes;

    private final Attributes failedAttributes;
    private final DoubleHistogram histogram;

    LatencyHistogram(Meter meter, String name, String description, String topic, Attributes attributes) {
        DoubleHistogramBuilder builder = meter.histogramBuilder(name)
                .setDescription(description)
                .setUnit(Unit.Seconds.toString())
                .setExplicitBucketBoundariesAdvice(latencyHistogramBuckets);

        if (topic != null) {
            ExtendedDoubleHistogramBuilder eb = (ExtendedDoubleHistogramBuilder) builder;
            eb.setAttributesAdvice(getDefaultAggregationLabels(attributes.toBuilder().put("success", true).build()));
            attributes = getTopicAttributes(topic, attributes);
        }

        successAttributes = attributes.toBuilder()
                .put("success", true)
                .build();
        failedAttributes = attributes.toBuilder()
                .put("success", false)
                .build();
        this.histogram = builder.build();
    }


    public void recordSuccess(long latencyNanos) {
        histogram.record(latencyNanos / NANOS, successAttributes);
    }

    public void recordFailure(long latencyNanos) {
        histogram.record(latencyNanos / NANOS, failedAttributes);
    }
}
