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

import static org.apache.curator.shaded.com.google.common.base.Preconditions.checkArgument;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.apache.bookkeeper.stats.Counter;

/**
 * {@link Counter} implementation based on {@link LongAdder}.
 *
 * <p>LongAdder keeps a counter per-thread and then aggregates to get the result, in order to avoid contention between
 * multiple threads.
 */
public class LongAdderCounter implements Counter {
    private final LongAdder counter = new LongAdder();

    public LongAdderCounter() {

    }

    @Override
    public void clear() {
        counter.reset();
    }

    @Override
    public void inc() {
        counter.increment();
    }

    @Override
    public void dec() {
        counter.decrement();
    }

    @Override
    public void addCount(long delta) {
        counter.add(delta);
    }

    @Override
    public void addLatency(long eventLatency, TimeUnit unit) {
        checkArgument(eventLatency >= 0);
        long valueMillis = unit.toMillis(eventLatency);
        counter.add(valueMillis);
    }

    @Override
    public Long get() {
        return counter.sum();
    }
}
