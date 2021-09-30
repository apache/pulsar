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
package org.apache.pulsar.broker.stats.prometheus.metrics;

import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.DoublesSketchBuilder;
import com.yahoo.sketches.quantiles.DoublesUnion;
import com.yahoo.sketches.quantiles.DoublesUnionBuilder;
import io.netty.util.concurrent.FastThreadLocal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.StampedLock;

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

        LocalData localData = current.localData.get();

        long stamp = localData.lock.readLock();
        try {
            localData.successSketch.update(valueMillis);
        } finally {
            localData.lock.unlockRead(stamp);
        }
    }

    public void rotateLatencyCollection() {
        // Swap current with replacement
        ThreadLocalAccessor local = current;
        current = replacement;
        replacement = local;

        final DoublesUnion aggregateValues = new DoublesUnionBuilder().build();
        local.map.forEach((localData, b) -> {
            long stamp = localData.lock.writeLock();
            try {
                aggregateValues.update(localData.successSketch);
                localData.successSketch.reset();
            } finally {
                localData.lock.unlockWrite(stamp);
            }
        });

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

    private static class LocalData {
        private final DoublesSketch successSketch = new DoublesSketchBuilder().build();
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
}