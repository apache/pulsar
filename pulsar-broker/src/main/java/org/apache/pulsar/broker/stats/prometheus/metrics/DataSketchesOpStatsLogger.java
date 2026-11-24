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
import java.util.concurrent.atomic.LongAdder;
import org.apache.bookkeeper.stats.OpStatsData;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.datasketches.kll.KllDoublesSketch;

/**
 * OpStatsLogger implementation that uses DataSketches library to calculate the approximated latency quantiles.
 */
public class DataSketchesOpStatsLogger implements OpStatsLogger {

    /**
     * Use 2 rotating thread local accessor so that we can safely swap them.
     */
    private volatile ThreadLocalAccessor current;
    private volatile ThreadLocalAccessor replacement;

    /**
     * These are the sketches where all the aggregated results are published.
     */
    private volatile KllDoublesSketch successResult;
    private volatile KllDoublesSketch failResult;

    private final LongAdder successCountAdder = new LongAdder();
    private final LongAdder failCountAdder = new LongAdder();

    private final LongAdder successSumAdder = new LongAdder();
    private final LongAdder failSumAdder = new LongAdder();

    public DataSketchesOpStatsLogger() {
        this.current = new ThreadLocalAccessor();
        this.replacement = new ThreadLocalAccessor();
    }

    @Override
    public void registerFailedEvent(long eventLatency, TimeUnit unit) {
        double valueMillis = unit.toMicros(eventLatency) / 1000.0;

        failCountAdder.increment();
        failSumAdder.add((long) valueMillis);
        current.getLocalData().updateFail(valueMillis);
    }

    @Override
    public void registerSuccessfulEvent(long eventLatency, TimeUnit unit) {
        double valueMillis = unit.toMicros(eventLatency) / 1000.0;

        successCountAdder.increment();
        successSumAdder.add((long) valueMillis);
        current.getLocalData().updateSuccess(valueMillis);
    }

    @Override
    public void registerSuccessfulValue(long value) {
        successCountAdder.increment();
        successSumAdder.add(value);
        current.getLocalData().updateSuccess(value);
    }

    @Override
    public void registerFailedValue(long value) {
        failCountAdder.increment();
        failSumAdder.add(value);
        current.getLocalData().updateFail(value);
    }

    @Override
    public OpStatsData toOpStatsData() {
        // Not relevant as we don't use JMX here
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        // Not relevant as we don't use JMX here
        throw new UnsupportedOperationException();
    }

    public void rotateLatencyCollection() {
        // Swap current with replacement
        ThreadLocalAccessor local = current;
        current = replacement;
        replacement = local;

        final KllDoublesSketch aggregateSuccess = KllDoublesSketch.newHeapInstance();
        final KllDoublesSketch aggregateFail = KllDoublesSketch.newHeapInstance();
        local.record(aggregateSuccess, aggregateFail);

        successResult = aggregateSuccess;
        failResult = aggregateFail;
    }

    public long getCount(boolean success) {
        return success ? successCountAdder.sum() : failCountAdder.sum();
    }

    public long getSum(boolean success) {
        return success ? successSumAdder.sum() : failSumAdder.sum();
    }

    public double getQuantileValue(boolean success, double quantile) {
        KllDoublesSketch s = success ? successResult : failResult;
        return (s != null && !s.isEmpty()) ? s.getQuantile(quantile) : Double.NaN;
    }
}
