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
package org.apache.pulsar.broker.stats.prometheus.metrics;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import org.apache.datasketches.kll.KllDoublesSketch;

public class DataSketchesSummaryLogger {

    /*
     * Use 2 rotating thread local accessor so that we can safely swap them.
     */
    private volatile ThreadLocalAccessor current;
    private volatile ThreadLocalAccessor replacement;

    /*
     * These are the sketches where all the aggregated results are published.
     */
    private volatile KllDoublesSketch values;
    private final LongAdder countAdder = new LongAdder();
    private final DoubleAdder sumAdder = new DoubleAdder();

    public DataSketchesSummaryLogger() {
        this.current = new ThreadLocalAccessor();
        this.replacement = new ThreadLocalAccessor();
    }

    public void registerEvent(long eventLatency, TimeUnit unit) {
        double valueMillis = unit.toMicros(eventLatency) / 1000.0;

        countAdder.increment();
        sumAdder.add(valueMillis);

        current.getLocalData().updateSuccess(valueMillis);
    }

    public void rotateLatencyCollection() {
        // Swap current with replacement
        ThreadLocalAccessor local = current;
        current = replacement;
        replacement = local;

        final KllDoublesSketch aggregateValues = KllDoublesSketch.newHeapInstance();
        local.record(aggregateValues, null);

        values = aggregateValues;
    }

    public long getCount() {
        return countAdder.sum();
    }

    public double getSum() {
        return sumAdder.sum();
    }

    public double getQuantileValue(double quantile) {
        KllDoublesSketch s = values;
        return (s != null && !s.isEmpty()) ? s.getQuantile(quantile) : Double.NaN;
    }
}
