/**
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

package org.apache.pulsar.functions.instance.stats;

import java.util.Arrays;
import org.apache.pulsar.functions.api.metrics.Histogram;

public class PrometheusHistogramImpl implements Histogram {

    private final io.prometheus.client.Histogram histogram;
    private final double[] buckets;

    PrometheusHistogramImpl(String name, String helpMessage, double[] buckets) {
        this.buckets = buckets;
        this.histogram = io.prometheus.client.Histogram.build()
                .name(name)
                .help(helpMessage)
                .buckets(buckets)
                .create();
    }

    @Override
    public void observe(double value) {
        histogram.observe(value);
    }

    @Override
    public double[] getBuckets() {
        return Arrays.copyOf(buckets, buckets.length);
    }

    @Override
    public double[] getValues() {
        return histogram.labels().get().buckets;
    }

    @Override
    public double getSum() {
        return histogram.labels().get().sum;
    }

    @Override
    public double getCount() {
        // the last Prometheus Histogram bucket is an inf bucket that essentially contains the count
        double[] bucketValues = histogram.labels().get().buckets;
        return bucketValues[bucketValues.length - 1];
    }
}
