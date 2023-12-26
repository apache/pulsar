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

package org.apache.pulsar.broker.qos;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@Fork(3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class AsyncTokenBucketBenchmark {
    private AsyncTokenBucket asyncTokenBucket;
    private DefaultMonotonicSnapshotClock monotonicSnapshotClock =
            new DefaultMonotonicSnapshotClock(TimeUnit.MILLISECONDS.toNanos(8), System::nanoTime);

    @Setup(Level.Iteration)
    public void setup() {
        long ratePerSecond = 100_000_000;
        asyncTokenBucket = AsyncTokenBucket.builder().rate(ratePerSecond).clock(monotonicSnapshotClock)
                .initialTokens(2 * ratePerSecond).capacity(2 * ratePerSecond).build();
    }

    @TearDown(Level.Iteration)
    public void teardown() {
        monotonicSnapshotClock.close();
    }

    @Threads(1)
    @Benchmark
    @Measurement(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
    @Warmup(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
    public void consumeTokensBenchmark001Threads() {
        asyncTokenBucket.consumeTokens(1);
    }

    @Threads(10)
    @Benchmark
    @Measurement(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
    @Warmup(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
    public void consumeTokensBenchmark010Threads() {
        asyncTokenBucket.consumeTokens(1);
    }

    @Threads(100)
    @Benchmark
    @Measurement(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
    @Warmup(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
    public void consumeTokensBenchmark100Threads() {
        asyncTokenBucket.consumeTokens(1);
    }
}
