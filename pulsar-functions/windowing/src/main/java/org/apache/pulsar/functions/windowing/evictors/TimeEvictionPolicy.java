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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.windowing.Event;
import org.apache.pulsar.functions.windowing.EvictionContext;
import org.apache.pulsar.functions.windowing.EvictionPolicy;

/**
 * Eviction policy that evicts events based on time duration.
 */
@Slf4j
public class TimeEvictionPolicy<T> implements EvictionPolicy<T, EvictionContext> {

    private final long windowLength;
    protected volatile EvictionContext evictionContext;
    private long delta;

    /**
     * Constructs a TimeEvictionPolicy that evicts events older
     * than the given window length in millis.
     *
     * @param windowLength the duration in milliseconds
     */
    public TimeEvictionPolicy(long windowLength) {
        this.windowLength = windowLength;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public EvictionPolicy.Action evict(Event<T> event) {
        long now =
                evictionContext == null ? System.currentTimeMillis() : evictionContext.getReferenceTime();
        long diff = now - event.getTimestamp();
        if (diff >= (windowLength + delta)) {
            return EvictionPolicy.Action.EXPIRE;
        } else if (diff < 0) { // do not process events beyond current ts
            return Action.KEEP;
        }
        return Action.PROCESS;
    }

    @Override
    public void track(Event<T> event) {
        // NOOP
    }

    @Override
    public void setContext(EvictionContext context) {
        EvictionContext prevContext = evictionContext;
        evictionContext = context;
        // compute window length adjustment (delta) to account for time drift
        if (context.getSlidingInterval() != null) {
            if (prevContext == null) {
                delta = Integer.MAX_VALUE; // consider all events for the initial window
            } else {
                delta = context.getReferenceTime() - prevContext.getReferenceTime()
                        - context.getSlidingInterval();
                if (Math.abs(delta) > 100) {
                    log.warn(String.format("Possible clock drift or long running computation in window; "
                            + "Previous eviction time: %s, current eviction time: %s", prevContext
                            .getReferenceTime(), context.getReferenceTime()));
                }
            }
        }
    }

    @Override
    public EvictionContext getContext() {
        return evictionContext;
    }

    @Override
    public void reset() {
        // NOOP
    }

    @Override
    public EvictionContext getState() {
        return evictionContext;
    }

    @Override
    public void restoreState(EvictionContext state) {
        this.evictionContext = state;
    }

    @Override
    public String toString() {
        return "TimeEvictionPolicy{" + "windowLength=" + windowLength + ", evictionContext="
                + evictionContext + '}';
    }
}
