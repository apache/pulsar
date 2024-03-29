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
package org.apache.pulsar.broker.stats;

import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public final class TimeWindow<T> {
    private final int interval;
    private final int sampleCount;
    private final AtomicReferenceArray<WindowWrap<T>> array;

    private final Lock updateLock = new ReentrantLock();

    public TimeWindow(int sampleCount, int interval) {
        this.sampleCount = sampleCount;
        this.interval = interval;
        this.array = new AtomicReferenceArray<>(sampleCount);
    }


    public WindowWrap<T> current(Function<T, T> function) {
        return current(function, System.currentTimeMillis());
    }


    /**
     * return current time window data.
     *
     * @param function generate data.
     * @return
     */
    public WindowWrap<T> current(Function<T, T> function, long timeMillis) {
        if (timeMillis < 0) {
            return null;
        }

        int idx = calculateTimeIdx(timeMillis);
        // Calculate current bucket start time.
        long windowStart = calculateWindowStart(timeMillis);
        while (true) {
            WindowWrap<T> old = array.get(idx);
            if (old == null) {
                WindowWrap<T> window = new WindowWrap<>(interval, windowStart, function.apply(null));
                if (array.compareAndSet(idx, null, window)) {
                    return window;
                } else {
                    // Contention failed, the thread will yield its time slice to wait for bucket available.
                    Thread.yield();
                }
            } else if (windowStart == old.start()) {
                return old;
            } else if (windowStart > old.start()) {
                if (updateLock.tryLock()) {
                    try {
                        // Successfully get the update lock, now we reset the bucket.
                        T value = null == function ? null : function.apply(old.value());
                        old.value(value);
                        old.resetWindowStart(windowStart);
                        return old;
                    } finally {
                        updateLock.unlock();
                    }
                } else {
                    // Contention failed, the thread will yield its time slice to wait for bucket available.
                    Thread.yield();
                }
            } else {
                //when windowStart < old.value()
                // Should not go through here, as the provided time is already behind.
                throw new IllegalStateException();
            }
        }
    }

    /**
     * return next time window data.
     *
     * @param function generate data.
     */
    public WindowWrap<T> next(Function<T, T> function, long timeMillis) {
        if (this.sampleCount <= 1) {
            throw new IllegalStateException("Argument sampleCount cannot less than 2");
        }
        return this.current(function, timeMillis + interval);
    }


    /**
     * return next time window data.
     *
     * @param function generate data.
     */
    public WindowWrap<T> next(Function<T, T> function) {
        return this.next(function, System.currentTimeMillis());
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
