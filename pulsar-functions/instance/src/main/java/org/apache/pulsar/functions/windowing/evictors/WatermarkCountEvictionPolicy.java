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
import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * An eviction policy that tracks count based on watermark ts and
 * evicts events up to the watermark based on a threshold count.
 *
 * @param <T> the type of event tracked by this policy.
 */
public class WatermarkCountEvictionPolicy<T>
        implements EvictionPolicy<T, Pair<Long, Long>> {
    protected final int threshold;
    protected final AtomicLong currentCount;
    private EvictionContext context;

    private static final AtomicLongFieldUpdater<WatermarkCountEvictionPolicy> PROCESSED_UPDATER =
            AtomicLongFieldUpdater.newUpdater(WatermarkCountEvictionPolicy.class, "processed");
    private volatile long processed;

    public WatermarkCountEvictionPolicy(int count) {
        threshold = count;
        currentCount = new AtomicLong();
    }

    public EvictionPolicy.Action evict(Event<T> event) {
        if (getContext() == null) {
            //It is possible to get asked about eviction before we have a context, due to WindowManager
            // .compactWindow.
            //In this case we should hold on to all the events. When the first watermark is received,
            // the context will be set,
            //and the events will be reevaluated for eviction
            return Action.STOP;
        }

        Action action;
        if (event.getTimestamp() <= getContext().getReferenceTime() && processed < currentCount.get()) {
            action = doEvict(event);
            if (action == Action.PROCESS) {
                PROCESSED_UPDATER.incrementAndGet(this);
            }
        } else {
            action = Action.KEEP;
        }
        return action;
    }

    private Action doEvict(Event<T> event) {
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
        // NOOP
    }

    @Override
    public EvictionContext getContext() {
        return context;
    }

    @Override
    public void setContext(EvictionContext context) {
        this.context = context;
        if (context.getCurrentCount() != null) {
            currentCount.set(context.getCurrentCount());
        } else {
            currentCount.set(processed + context.getSlidingCount());
        }
        processed = 0;
    }

    @Override
    public void reset() {
        processed = 0;
    }

    @Override
    public Pair<Long, Long> getState() {
        return Pair.of(currentCount.get(), processed);
    }

    @Override
    public void restoreState(Pair<Long, Long> state) {
        currentCount.set(state.getLeft());
        processed = state.getRight();
    }

    @Override
    public String toString() {
        return "WatermarkCountEvictionPolicy{" + "} " + super.toString();
    }
}
