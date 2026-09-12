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
package org.apache.pulsar.client.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
import org.openjdk.jmh.annotations.Warmup;

/** Isolates round-robin list traversal; real perf-producer payload, metrics and asynchronous sends are omitted. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class ProducerRoundRobinBenchmark {
    @Param({"1", "8", "64"})
    public int producerCount;

    @Param({"false", "true"})
    public boolean indexed;

    private List<ProducerSlot> producers;

    @Setup
    public void setup() {
        producers = new ArrayList<>(producerCount);
        for (int i = 0; i < producerCount; i++) {
            producers.add(new ProducerSlot());
        }
    }

    @Benchmark
    public long visitProducers() {
        long sent = 0;
        if (indexed) {
            for (int i = 0; i < producers.size(); i++) {
                sent += producers.get(i).send();
            }
        } else {
            for (ProducerSlot producer : producers) {
                sent += producer.send();
            }
        }
        return sent;
    }

    private static final class ProducerSlot {
        private long completed;

        long send() {
            return ++completed;
        }
    }
}
