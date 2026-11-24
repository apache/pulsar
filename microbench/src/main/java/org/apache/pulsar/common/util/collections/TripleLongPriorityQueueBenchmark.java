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

package org.apache.pulsar.common.util.collections;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * JMH benchmarks for {@link TripleLongPriorityQueue} simulating Pulsar delayed delivery workloads.
 *
 * <p>Three scenarios matching real usage:
 * <ul>
 *   <li>{@link #recoveryBulkAddThenPop} — snapshot recovery: bulk add all entries, then pop all.
 *       Cold cache, large heap. This is the <b>worst case</b> for the hole-based optimization
 *       because cache misses dominate over reduced readLong calls.</li>
 *   <li>{@link #interleavedAddPop} — steady-state delayed delivery: batch add (messages arriving
 *       between timer ticks), then batch pop (getScheduledMessages). Heap stays warm in cache.
 *       This is the <b>primary hot path</b>.</li>
 *   <li>{@link #steadyState} — constant-depth steady state: pre-fill queue, then alternating
 *       small add/pop batches. Simulates sustained throughput with ~10K queue depth.</li>
 * </ul>
 *
 * <p>Build and run:
 * <pre>
 * ./gradlew :microbench:shadowJar
 * java -jar microbench/build/libs/microbench-*-benchmarks.jar ".*TripleLongPriorityQueue.*"
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class TripleLongPriorityQueueBenchmark {

    @Param({"50000", "500000", "2000000"})
    int size;

    /**
     * Recovery scenario: bulk add all then pop all.
     * Simulates snapshot recovery from BookKeeper — cold cache, large heap.
     */
    @Benchmark
    public void recoveryBulkAddThenPop(Blackhole bh) {
        try (TripleLongPriorityQueue pq = new TripleLongPriorityQueue()) {
            long baseTs = System.currentTimeMillis();
            Random rng = new Random(42);
            for (int i = 0; i < size; i++) {
                long n1 = baseTs + rng.nextLong(3_600_000);
                long n2 = i / 1000;
                long n3 = i;
                pq.add(n1, n2, n3);
            }
            while (!pq.isEmpty()) {
                bh.consume(pq.peekN1());
                pq.pop();
            }
        }
    }

    /**
     * Interleaved scenario: batch add then batch pop, repeated.
     * Simulates steady-state delayed delivery — messages arrive continuously,
     * getScheduledMessages pops in batches of ~500 when consumers are ready.
     * Heap is warm in L2/L3 cache between operations.
     */
    @Benchmark
    public void interleavedAddPop(Blackhole bh) {
        try (TripleLongPriorityQueue pq = new TripleLongPriorityQueue()) {
            Random rng = new Random(42);
            long baseTs = System.currentTimeMillis();
            int batchSize = 500;
            int totalAdded = 0;

            while (totalAdded < size) {
                // Batch add: messages arriving between timer ticks
                int addCount = Math.min(batchSize + rng.nextInt(500), size - totalAdded);
                for (int i = 0; i < addCount; i++) {
                    long n1 = baseTs + rng.nextLong(3_600_000);
                    long n2 = (totalAdded + i) / 1000;
                    long n3 = totalAdded + i;
                    pq.add(n1, n2, n3);
                }
                totalAdded += addCount;

                // Batch pop: getScheduledMessages delivering to consumers
                int popCount = (int) Math.min(batchSize, pq.size());
                for (int i = 0; i < popCount; i++) {
                    bh.consume(pq.peekN1());
                    pq.pop();
                }
            }
            // Drain remaining
            while (!pq.isEmpty()) {
                bh.consume(pq.peekN1());
                pq.pop();
            }
        }
    }

    /**
     * Steady-state scenario: pre-fill queue, then alternating small add/pop.
     * Simulates sustained throughput with constant ~10K queue depth.
     * Heap stays hot in cache — this is where the readLong reduction matters most.
     */
    @Benchmark
    public void steadyState(Blackhole bh) {
        int steadyDepth = 10_000;
        try (TripleLongPriorityQueue pq = new TripleLongPriorityQueue()) {
            Random rng = new Random(42);
            long baseTs = System.currentTimeMillis();
            long seq = 0;

            // Pre-fill
            for (int i = 0; i < steadyDepth; i++) {
                pq.add(baseTs + rng.nextLong(3_600_000), seq / 1000, seq);
                seq++;
            }

            // Alternating add/pop to maintain steady depth
            int ops = 0;
            while (ops < size) {
                // Small batch add
                int addCount = 100 + rng.nextInt(100);
                for (int i = 0; i < addCount && ops < size; i++) {
                    pq.add(baseTs + rng.nextLong(3_600_000), seq / 1000, seq);
                    seq++;
                    ops++;
                }
                // Small batch pop
                int popCount = addCount - rng.nextInt(20);
                for (int i = 0; i < popCount && !pq.isEmpty(); i++) {
                    bh.consume(pq.peekN1());
                    pq.pop();
                }
            }
        }
    }
}
