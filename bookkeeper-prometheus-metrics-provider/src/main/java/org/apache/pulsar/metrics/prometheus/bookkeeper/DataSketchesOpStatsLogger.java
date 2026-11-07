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
package org.apache.pulsar.metrics.prometheus.bookkeeper;

import io.netty.util.concurrent.FastThreadLocal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.StampedLock;
import org.apache.bookkeeper.stats.OpStatsData;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.datasketches.kll.KllDoublesSketch;

/**
 * OpStatsLogger implementation that uses DataSketches library to calculate the approximated latency quantiles.
 */
public class DataSketchesOpStatsLogger implements OpStatsLogger {

    /*
     * Use 2 rotating thread local accessor so that we can safely swap them.
     */
    private volatile ThreadLocalAccessor current;
    private volatile ThreadLocalAccessor replacement;

    /*
     * These are the sketches where all the aggregated results are published.
     */
    private volatile KllDoublesSketch successResult;
    private volatile KllDoublesSketch failResult;

    private final LongAdder successCountAdder = new LongAdder();
    private final LongAdder failCountAdder = new LongAdder();

    private final LongAdder successSumAdder = new LongAdder();
    private final LongAdder failSumAdder = new LongAdder();

    private Map<String, String> labels;

    // used for lazy registration for thread scoped metrics
    private boolean threadInitialized;

    public DataSketchesOpStatsLogger(Map<String, String> labels) {
        this.current = new ThreadLocalAccessor();
        this.replacement = new ThreadLocalAccessor();
        this.labels = labels;
    }

    @Override
    public void registerFailedEvent(long eventLatency, TimeUnit unit) {
        double valueMillis = unit.toMicros(eventLatency) / 1000.0;

        failCountAdder.increment();
        failSumAdder.add((long) valueMillis);

        LocalData localData = current.localData.get();

        long stamp = localData.lock.readLock();
        try {
            localData.failSketch.update(valueMillis);
        } finally {
            localData.lock.unlockRead(stamp);
        }
    }

    @Override
    public void registerSuccessfulEvent(long eventLatency, TimeUnit unit) {
        double valueMillis = unit.toMicros(eventLatency) / 1000.0;

        successCountAdder.increment();
        successSumAdder.add((long) valueMillis);

        LocalData localData = current.localData.get();

        long stamp = localData.lock.readLock();
        try {
            localData.successSketch.update(valueMillis);
        } finally {
            localData.lock.unlockRead(stamp);
        }
    }

    @Override
    public void registerSuccessfulValue(long value) {
        successCountAdder.increment();
        successSumAdder.add(value);

        LocalData localData = current.localData.get();

        long stamp = localData.lock.readLock();
        try {
            localData.successSketch.update(value);
        } finally {
            localData.lock.unlockRead(stamp);
        }
    }

    @Override
    public void registerFailedValue(long value) {
        failCountAdder.increment();
        failSumAdder.add(value);

        LocalData localData = current.localData.get();

        long stamp = localData.lock.readLock();
        try {
            localData.failSketch.update(value);
        } finally {
            localData.lock.unlockRead(stamp);
        }
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
        local.map.forEach((localData, b) -> {
            long stamp = localData.lock.writeLock();
            try {
                aggregateSuccess.merge(localData.successSketch);
                aggregateFail.merge(localData.failSketch);
                localData.successSketch = KllDoublesSketch.newHeapInstance();
                localData.failSketch = KllDoublesSketch.newHeapInstance();
            } finally {
                localData.lock.unlockWrite(stamp);
            }
        });

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

    public Map<String, String> getLabels() {
        return labels;
    }

    public boolean isThreadInitialized() {
        return threadInitialized;
    }

    public void initializeThread(Map<String, String> labels) {
        this.labels = labels;
        this.threadInitialized = true;
    }

    private static class LocalData {
        private KllDoublesSketch successSketch = KllDoublesSketch.newHeapInstance();
        private KllDoublesSketch failSketch = KllDoublesSketch.newHeapInstance();
        private final StampedLock lock = new StampedLock();
    }

    private static class ThreadLocalAccessor {
        private final Map<LocalData, Boolean> map = new ConcurrentHashMap<>();
        private final FastThreadLocal<LocalData> localData = new FastThreadLocal<LocalData>() {

            @Override
            protected LocalData initialValue() throws Exception {
                LocalData localData = new LocalData();
                map.put(localData, Boolean.TRUE);
                return localData;
            }

            @Override
            protected void onRemoval(LocalData value) throws Exception {
                map.remove(value);
            }
        };
    }

    @Override
    public String toString() {
        return "DataSketchesOpStatsLogger{labels=" + labels + ", id=" + System.identityHashCode(this) + "}";
    }
}
