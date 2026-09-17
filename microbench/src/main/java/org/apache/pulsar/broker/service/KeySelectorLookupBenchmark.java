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
package org.apache.pulsar.broker.service;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class KeySelectorLookupBenchmark {
    // The lightweight Consumer constructor has no connection; provide identity equality for membership tracking.
    private static final class NamedConsumer extends Consumer {
        NamedConsumer(String name) {
            super(name, 1000);
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(this);
        }

        @Override
        public boolean equals(Object other) {
            return this == other;
        }
    }

    @State(Scope.Benchmark)
    public static class Ring {
        @Param({"2", "50"})
        public int consumers;
        ConsistentHashingStickyKeyConsumerSelector selector;
        Consumer changing;

        @Setup
        public void setup() {
            selector = new ConsistentHashingStickyKeyConsumerSelector(100, true);
            changing = new NamedConsumer("membership-change");
            for (int i = 0; i < consumers; i++) {
                selector.addConsumer(new NamedConsumer("benchmark-" + i)).join();
            }
        }
    }

    @State(Scope.Thread)
    public static class Keys {
        int next = 12345;

        int nextHash() {
            next ^= next << 13;
            next ^= next >>> 17;
            next ^= next << 5;
            return 1 + (next & Integer.MAX_VALUE) % 65535;
        }
    }

    /** Run with one benchmark thread: measures the cost moved to membership updates. */
    @Benchmark
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void changeMembership(Ring ring) {
        ring.selector.addConsumer(ring.changing).join();
        ring.selector.removeConsumer(ring.changing);
    }

    @Benchmark
    public Consumer select(Ring ring, Keys keys) {
        return ring.selector.select(keys.nextHash());
    }
}
