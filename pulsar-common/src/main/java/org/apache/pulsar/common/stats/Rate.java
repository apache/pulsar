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
package org.apache.pulsar.common.stats;

import static com.google.common.base.Preconditions.checkArgument;
import java.math.BigDecimal;
import java.util.concurrent.atomic.LongAdder;

/**
 */
public class Rate {
    // Counters
    private final LongAdder valueAdder = new LongAdder();
    private final LongAdder countAdder = new LongAdder();
    private final LongAdder totalCountAdder = new LongAdder();

    // Computed stats
    private long count = 0L;
    private double rate = 0.0d;
    private double valueRate = 0.0d;
    private double averageValue = 0.0d;
    private long lastCalculatedTime = System.nanoTime();

    public void recordEvent() {
        countAdder.increment();
        totalCountAdder.increment();
    }

    public void recordEvent(long value) {
        valueAdder.add(value);
        countAdder.increment();
        totalCountAdder.increment();
    }

    public void recordMultipleEvents(long events, long totalValue) {
        valueAdder.add(totalValue);
        countAdder.add(events);
        totalCountAdder.add(events);
    }

    public void calculateRate() {
        double period = (System.nanoTime() - lastCalculatedTime) / 1e9;
        calculateRate(period);
        lastCalculatedTime = System.nanoTime();
    }

    public void calculateRate(double period) {
        checkArgument(period > 0, "Invalid period %s to calculate rate", period);

        count = countAdder.sumThenReset();
        long sum = valueAdder.sumThenReset();
        averageValue = count != 0 ? Long.valueOf(sum).doubleValue() / Long.valueOf(count).doubleValue() : 0.0d;
        rate = count / period;
        valueRate = sum / period;
    }

    public long getCount() {
        return count;
    }

    public double getAverageValue() {
        return averageValue;
    }

    public double getRate() {
        return rate;
    }

    public double getValueRate() {
        return valueRate;
    }

    public long getTotalCount() {
        return this.totalCountAdder.longValue();
    }
}
