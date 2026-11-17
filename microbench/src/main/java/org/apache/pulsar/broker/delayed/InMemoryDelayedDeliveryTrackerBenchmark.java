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
package org.apache.pulsar.broker.delayed;

import static org.mockito.Mockito.mock;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures membership checks during a cursor rewind and the cost of maintaining both delayed-message indexes.
 * Entries span multiple ledgers and delivery-time buckets. This excludes storage reads and broker dispatch.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
public class InMemoryDelayedDeliveryTrackerBenchmark {
    private static final long DELIVERY_TIME = 1_000_000_000_000L;
    private static final int ENTRIES_PER_LEDGER = 1000;

    @Param({"1000", "100000"})
    public int messages;

    private Timer timer;
    private MutableClock clock;
    private InMemoryDelayedDeliveryTracker tracker;
    private int lookupIndex;

    @Setup
    public void setup() {
        timer = new HashedWheelTimer();
        clock = new MutableClock();
        tracker = new InMemoryDelayedDeliveryTracker(mock(AbstractPersistentDispatcherMultipleConsumers.class),
                timer, 1000, clock, true, 0);
        addMessages();
    }

    private void addMessages() {
        for (int i = 0; i < messages; i++) {
            tracker.addMessage(i / ENTRIES_PER_LEDGER, i % ENTRIES_PER_LEDGER,
                    DELIVERY_TIME + (i % 100) * 1000L);
        }
    }

    @Benchmark
    public boolean containsMessage() {
        int index = lookupIndex++;
        if (lookupIndex == messages * 2) {
            lookupIndex = 0;
        }
        // Half the positions are indexed; the other half have the same ledgers and untracked entry ids.
        return tracker.containsMessage(index / (ENTRIES_PER_LEDGER * 2), index % (ENTRIES_PER_LEDGER * 2));
    }

    @Benchmark
    public long addAndDrain() {
        tracker.clear();
        clock.now = 0;
        addMessages();
        clock.now = DELIVERY_TIME + 100_000;
        int drained = tracker.getScheduledMessages(messages).size();
        if (drained != messages) {
            throw new IllegalStateException("Not all indexed messages were drained: " + drained);
        }
        return drained;
    }

    @TearDown
    public void tearDown() {
        tracker.close();
        timer.stop();
    }

    private static final class MutableClock extends Clock {
        private long now;

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return Clock.fixed(instant(), zone);
        }

        @Override
        public long millis() {
            return now;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(now);
        }
    }
}
