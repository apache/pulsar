/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.mledger.util;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import java.util.concurrent.TimeUnit;

/**
 */
public class DimensionStats {

    /** Statistics for given dimension **/
    public double meanDimensionMs;

    public double medianDimensionMs;

    public double dimension95Ms;

    public double dimension99Ms;

    public double dimension999Ms;

    public double dimension9999Ms;

    public double dimensionCounts;

    public double elapsedIntervalMs;

    private final long maxTrackableSeconds = 120;
    private Recorder dimensionTimeRecorder = new Recorder(TimeUnit.SECONDS.toMillis(maxTrackableSeconds), 2);
    private Histogram dimensionHistogram = null;

    public void updateStats() {

        dimensionHistogram = dimensionTimeRecorder.getIntervalHistogram(dimensionHistogram);

        this.meanDimensionMs = dimensionHistogram.getMean();
        this.medianDimensionMs = dimensionHistogram.getValueAtPercentile(50);
        this.dimension95Ms = dimensionHistogram.getValueAtPercentile(95);
        this.dimension99Ms = dimensionHistogram.getValueAtPercentile(99);
        this.dimension999Ms = dimensionHistogram.getValueAtPercentile(99.9);
        this.dimension9999Ms = dimensionHistogram.getValueAtPercentile(99.99);
        this.dimensionCounts = dimensionHistogram.getTotalCount();
    }

    public void recordValue(long dimensionLatencyMs) {
        dimensionLatencyMs = dimensionLatencyMs > maxTrackableSeconds ? maxTrackableSeconds : dimensionLatencyMs;
        dimensionTimeRecorder.recordValue(dimensionLatencyMs);
    }
}
