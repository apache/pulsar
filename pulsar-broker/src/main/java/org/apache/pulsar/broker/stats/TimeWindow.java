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

import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Function;

public final class TimeWindow<T> {
    private final int interval;
    private final int sampleCount;
    private final AtomicReferenceArray<WindowWrap<T>> array;

    public TimeWindow(int sampleCount, int interval) {
        this.sampleCount = sampleCount;
        this.interval = interval;
        this.array = new AtomicReferenceArray<>(sampleCount);
    }

    /**
     * return current time window data.
     *
     * @param function generate data.
     * @return
     */
    public synchronized WindowWrap<T> current(Function<T, T> function) {
        long millis = System.currentTimeMillis();

        if (millis < 0) {
            return null;
        }
        int idx = calculateTimeIdx(millis);
        long windowStart = calculateWindowStart(millis);
        while (true) {
            WindowWrap<T> old = array.get(idx);
            if (old == null) {
                WindowWrap<T> window = new WindowWrap<>(interval, windowStart, null);
                if (array.compareAndSet(idx, null, window)) {
                    T value = null == function ? null : function.apply(null);
                    window.value(value);
                    return window;
                } else {
                    Thread.yield();
                }
            } else if (windowStart == old.start()) {
                return old;
            } else if (windowStart > old.start()) {
                T value = null == function ? null : function.apply(old.value());
                old.value(value);
                old.resetWindowStart(windowStart);
                return old;
            } else {
                //it should never goes here
                throw new IllegalStateException();
            }
        }
    }

    private int calculateTimeIdx(long timeMillis) {
        long timeId = timeMillis / this.interval;
        return (int) (timeId % sampleCount);
    }

    private long calculateWindowStart(long timeMillis) {
        return timeMillis - timeMillis % this.interval;
    }

    public int sampleCount() {
        return sampleCount;
    }

    public int interval() {
        return interval;
    }

    public long currentWindowStart(long millis) {
        return this.calculateWindowStart(millis);
    }
}
