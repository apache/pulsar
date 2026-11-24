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
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * On MacOS, the performance of System.nanoTime() is not great. Running benchmarks on Linux is recommended due
 * to the bottleneck of System.nanoTime() implementation on MacOS.
 */
@Fork(3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class DefaultMonotonicClockBenchmark {
    private DefaultMonotonicClock monotonicSnapshotClock =
            new DefaultMonotonicClock();

    @Threads(1)
    @Benchmark
    @Measurement(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
    @Warmup(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
    public void getTickNanos001Threads(Blackhole blackhole) {
        blackhole.consume(monotonicSnapshotClock.getTickNanos());
    }

    @Threads(10)
    @Benchmark
    @Measurement(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
    @Warmup(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
    public void getTickNanos010Threads(Blackhole blackhole) {
        blackhole.consume(monotonicSnapshotClock.getTickNanos());
    }

    @Threads(100)
    @Benchmark
    @Measurement(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
    @Warmup(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
    public void getTickNanos100Threads(Blackhole blackhole) {
        blackhole.consume(monotonicSnapshotClock.getTickNanos());
    }
}
