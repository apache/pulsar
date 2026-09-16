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
package org.apache.pulsar.common.util;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.util.collections.GrowableArrayBlockingQueue;
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
 * Measures the cost of regrowing after a full drain and a maintenance pass. Each invocation models
 * a separate burst, bypassing the production 30-second delay. This deliberately measures the costly
 * repeated-burst case, not steady executor throughput or the expected frequency of maintenance.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class GrowableQueueTrimBenchmark {
    @Param({"1024", "65536"})
    public int burstSize;

    @Param({"false", "true"})
    public boolean trimAfterBurst;

    private final Object element = new Object();
    private Queue<Runnable> passes;
    private ExecutorQueueTrimmer group;
    private GrowableArrayBlockingQueue<Object> queue;

    @Setup
    public void setup() {
        passes = new ArrayDeque<>();
        group = new ExecutorQueueTrimmer(burstSize / 4, burstSize / 2, passes::add);
        queue = group.newQueue();
    }

    @Benchmark
    public int burstDrainAndMaintain() {
        for (int i = 0; i < burstSize; i++) {
            queue.add(element);
        }
        for (int i = 0; i < burstSize; i++) {
            if (queue.poll() != element) {
                throw new IllegalStateException("Lost queued element");
            }
        }
        if (trimAfterBurst) {
            passes.remove().run();
        }
        return queue.capacity();
    }

    @TearDown
    public void tearDown() {
        if (!queue.isEmpty() || queue.capacity() != (trimAfterBurst ? 64 : burstSize)) {
            throw new IllegalStateException("Unexpected retained queue capacity");
        }
        group.unregister(queue);
    }
}
