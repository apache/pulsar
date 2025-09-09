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

import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.DoublesUnion;
import com.yahoo.sketches.quantiles.DoublesUnionBuilder;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class DataSketchesSummaryLogger {

    /*
     * Use 2 rotating thread local accessor so that we can safely swap them.
     */
    private volatile ThreadLocalAccessor current;
    private volatile ThreadLocalAccessor replacement;

    /*
     * These are the sketches where all the aggregated results are published.
     */
    private volatile DoublesSketch values;
    private final LongAdder countAdder = new LongAdder();
    private final LongAdder sumAdder = new LongAdder();

    public DataSketchesSummaryLogger() {
        this.current = new ThreadLocalAccessor();
        this.replacement = new ThreadLocalAccessor();
    }

    public void registerEvent(long eventLatency, TimeUnit unit) {
        double valueMillis = unit.toMicros(eventLatency) / 1000.0;

        countAdder.increment();
        sumAdder.add((long) valueMillis);

        current.getLocalData().updateSuccess(valueMillis);
    }

    public void rotateLatencyCollection() {
        // Swap current with replacement
        ThreadLocalAccessor local = current;
        current = replacement;
        replacement = local;

        final DoublesUnion aggregateValues = new DoublesUnionBuilder().build();
        local.record(aggregateValues, null);

        values = aggregateValues.getResultAndReset();
    }

    public long getCount() {
        return countAdder.sum();
    }

    public long getSum() {
        return sumAdder.sum();
    }

    public double getQuantileValue(double quantile) {
        DoublesSketch s = values;
        return s != null ? s.getQuantile(quantile) : Double.NaN;
    }
}
