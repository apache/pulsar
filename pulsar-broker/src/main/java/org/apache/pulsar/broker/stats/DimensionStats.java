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
package org.apache.pulsar.broker.stats;

import static io.prometheus.client.CollectorRegistry.defaultRegistry;

import java.util.concurrent.TimeUnit;

import io.prometheus.client.Collector;
import io.prometheus.client.Summary;
import io.prometheus.client.Summary.Builder;

/**
 */
public class DimensionStats {

    private final String name;
    private final Summary summary;
    private static final double[] quantiles = { 0.50, 0.75, 0.95, 0.99, 0.999, 0.9999 };

    public DimensionStats(String name, long updateDurationInSec) {
        this.name = name;
        Builder summaryBuilder = Summary.build().name(name).help("-");
        for (int i = 0; i < quantiles.length; i++) {
            summaryBuilder.quantile(quantiles[i], 0.01);
        }
        this.summary = summaryBuilder.maxAgeSeconds(updateDurationInSec).create().register(defaultRegistry);
    }

    public void recordDimensionTimeValue(long latency, TimeUnit unit) {
        summary.observe(unit.toMillis(latency));
    }

    public double getMeanDimension() {
        double sum = getDimensionSum();
        double count = getDimensionCount();
        if (!Double.isNaN(sum) && !Double.isNaN(count)) {
            return sum / count;
        }
        return 0;
    }

    public double getMedianDimension() {
        return getQuantile(quantiles[0]);
    }

    public double getDimension75() {
        return getQuantile(quantiles[1]);
    }

    public double getDimension95() {
        return getQuantile(quantiles[2]);
    }

    public double getDimension99() {
        return getQuantile(quantiles[3]);
    }

    public double getDimension999() {
        return getQuantile(quantiles[4]);
    }

    public double getDimension9999() {
        return getQuantile(quantiles[5]);
    }

    public double getDimensionSum() {
        return defaultRegistry.getSampleValue(name + "_sum").doubleValue();
    }

    public double getDimensionCount() {
        return defaultRegistry.getSampleValue(name + "_count").doubleValue();
    }

    private double getQuantile(double q) {
        return defaultRegistry
                .getSampleValue(name, new String[] { "quantile" }, new String[] { Collector.doubleToGoString(q) })
                .doubleValue();
    }
}
