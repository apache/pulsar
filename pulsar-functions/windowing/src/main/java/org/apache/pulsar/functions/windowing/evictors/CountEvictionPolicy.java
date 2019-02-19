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
package org.apache.pulsar.functions.windowing.evictors;

import org.apache.pulsar.functions.windowing.Event;
import org.apache.pulsar.functions.windowing.EvictionContext;
import org.apache.pulsar.functions.windowing.EvictionPolicy;

import java.util.concurrent.atomic.AtomicLong;

/**
 * An eviction policy that tracks event counts and can
 * evict based on a threshold count.
 *
 * @param <T> the type of event tracked by this policy.
 */
public class CountEvictionPolicy<T> implements EvictionPolicy<T, Long> {
    protected final int threshold;
    protected final AtomicLong currentCount;
    private EvictionContext context;

    public CountEvictionPolicy(int count) {
        this.threshold = count;
        this.currentCount = new AtomicLong();
    }

    @Override
    public Action evict(Event<T> event) {
        /*
         * atomically decrement the count if its greater than threshold and
         * return if the event should be evicted
         */
        while (true) {
            long curVal = currentCount.get();
            if (curVal > threshold) {
                if (currentCount.compareAndSet(curVal, curVal - 1)) {
                    return Action.EXPIRE;
                }
            } else {
                break;
            }
        }
        return Action.PROCESS;
    }

    @Override
    public void track(Event<T> event) {
        if (!event.isWatermark()) {
            currentCount.incrementAndGet();
        }
    }

    @Override
    public void setContext(EvictionContext context) {
        this.context = context;
    }

    @Override
    public EvictionContext getContext() {
        return context;
    }

    @Override
    public String toString() {
        return "CountEvictionPolicy{" + "threshold=" + threshold + ", currentCount=" + currentCount
                + '}';
    }

    @Override
    public void reset() {
        // NOOP
    }

    @Override
    public Long getState() {
        return currentCount.get();
    }

    @Override
    public void restoreState(Long state) {
        currentCount.set(state);
    }
}
