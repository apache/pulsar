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

/**
 * An eviction policy that evicts events based on time duration taking
 * watermark time and event lag into account.
 */
public class WatermarkTimeEvictionPolicy<T> extends TimeEvictionPolicy<T> {
    private final long lag;

    /**
     * Constructs a WatermarkTimeEvictionPolicy that evicts events older
     * than the given window length in millis.
     *
     * @param windowLength the window length in milliseconds
     */
    public WatermarkTimeEvictionPolicy(long windowLength) {
        this(windowLength, Long.MAX_VALUE);
    }

    /**
     * Constructs a WatermarkTimeEvictionPolicy that evicts events older
     * than the given window length in millis. The lag parameter
     * can be used in the case of event based ts to break the queue
     * scan early.
     *
     * @param windowLength the window length in milliseconds
     * @param lag the max event lag in milliseconds
     */
    public WatermarkTimeEvictionPolicy(long windowLength, long lag) {
        super(windowLength);
        this.lag = lag;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Keeps events with future ts in the queue for processing in the next
     * window. If the ts difference is more than the lag, stops scanning
     * the queue for the current window.
     */
    @Override
    public Action evict(Event<T> event) {
        if (evictionContext == null) {
            //It is possible to get asked about eviction before we have a context, due to WindowManager
            // .compactWindow.
            //In this case we should hold on to all the events. When the first watermark is received,
            // the context will be set,
            //and the events will be reevaluated for eviction
            return Action.STOP;
        }

        long referenceTime = evictionContext.getReferenceTime();
        long diff = referenceTime - event.getTimestamp();
        if (diff < -lag) {
            return Action.STOP;
        } else if (diff < 0) {
            return Action.KEEP;
        } else {
            return super.evict(event);
        }
    }

    @Override
    public String toString() {
        return "WatermarkTimeEvictionPolicy{" + "lag=" + lag + "} " + super.toString();
    }

}
