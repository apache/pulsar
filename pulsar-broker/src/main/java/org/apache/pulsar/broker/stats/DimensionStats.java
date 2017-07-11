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

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import java.util.concurrent.TimeUnit;

/**
 */
public class DimensionStats {

    /** Statistics for dimension times **/
    public double meanDimensionMs;

    public double medianDimensionMs;

    public double dimension95Ms;

    public double dimension99Ms;

    public double dimension999Ms;

    public double dimension9999Ms;

    public double dimensionCounts;

    public double elapsedIntervalMs;

    private Recorder dimensionTimeRecorder = new Recorder(TimeUnit.MINUTES.toMillis(10), 2);
    private Histogram dimensionHistogram = null;
    private double dimensionRecordStartTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());

    public void updateStats() {

        dimensionHistogram = dimensionTimeRecorder.getIntervalHistogram(dimensionHistogram);
        this.elapsedIntervalMs = (TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) - dimensionRecordStartTime);
        dimensionRecordStartTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());

        this.meanDimensionMs = dimensionHistogram.getMean();
        this.medianDimensionMs = dimensionHistogram.getValueAtPercentile(50);
        this.dimension95Ms = dimensionHistogram.getValueAtPercentile(95);
        this.dimension99Ms = dimensionHistogram.getValueAtPercentile(99);
        this.dimension999Ms = dimensionHistogram.getValueAtPercentile(99.9);
        this.dimension9999Ms = dimensionHistogram.getValueAtPercentile(99.99);
        this.dimensionCounts = dimensionHistogram.getTotalCount();
    }

    public void recordDimensionTimeValue(long latency, TimeUnit unit) {
        dimensionTimeRecorder.recordValue(unit.toMillis(latency));
    }
}
