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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.prometheus.client.Collector;
import io.prometheus.client.Summary;
import io.prometheus.client.Summary.Builder;

/**
 */
public class DimensionStats {

    private final String name;
    private final String dimensionSumLabel;
    private final String dimensionCountLabel;
    private final Summary summary;
    private static final double[] QUANTILES = { 0.50, 0.75, 0.95, 0.99, 0.999, 0.9999 };
    private static final String[] QUANTILE_LABEL = { "quantile" };

    public DimensionStats(String name, long updateDurationInSec) {
        this.name = name;
        this.dimensionSumLabel = name + "_sum";
        this.dimensionCountLabel = name + "_count";
        Builder summaryBuilder = Summary.build().name(name).help("-");
        for (int i = 0; i < QUANTILES.length; i++) {
            summaryBuilder.quantile(QUANTILES[i], 0.01);
        }
        this.summary = summaryBuilder.maxAgeSeconds(updateDurationInSec).create();
        try {
            defaultRegistry.register(summary);
        } catch (IllegalArgumentException ie) {
            // it only happens in test-cases when try to register summary multiple times in registry
            log.warn("{} is already registred {}", name, ie.getMessage());
        }
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
        return getQuantile(QUANTILES[0]);
    }

    public double getDimension75() {
        return getQuantile(QUANTILES[1]);
    }

    public double getDimension95() {
        return getQuantile(QUANTILES[2]);
    }

    public double getDimension99() {
        return getQuantile(QUANTILES[3]);
    }

    public double getDimension999() {
        return getQuantile(QUANTILES[4]);
    }

    public double getDimension9999() {
        return getQuantile(QUANTILES[5]);
    }

    public double getDimensionSum() {
        return defaultRegistry.getSampleValue(dimensionSumLabel).doubleValue();
    }

    public double getDimensionCount() {
        return defaultRegistry.getSampleValue(dimensionCountLabel).doubleValue();
    }

    private double getQuantile(double q) {
        return defaultRegistry.getSampleValue(name, QUANTILE_LABEL, new String[] { Collector.doubleToGoString(q) })
                .doubleValue();
    }
    
    private static final Logger log = LoggerFactory.getLogger(DimensionStats.class);
}
