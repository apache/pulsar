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
package org.apache.pulsar.broker.topiclistlimit;

import io.opentelemetry.api.OpenTelemetry;
import io.prometheus.client.CollectorRegistry;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiter.AsyncDualMemoryLimiterPermit;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiter.LimitType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures what acquiring and releasing direct memory permits costs the calling thread when permits are available,
 * as for every topic list response and watcher update that a Netty IO thread sends. The requests never wait, so the
 * score shows the cost of recording their wait time, and of contending on it with 12 threads, like a broker with 12
 * Netty IO threads. A recording cost that grows with the number of observations shows up as iteration scores that
 * keep decreasing. Compare revisions by running the same benchmark on each.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class TopicListMemoryLimiterBenchmark {
    private static final long MAX_MEMORY = 100L * 1024 * 1024;
    private static final BooleanSupplier NOT_CANCELLED = () -> false;

    private TopicListMemoryLimiter limiter;

    @Setup
    public void setup() {
        // broker defaults, the OpenTelemetry SDK is disabled by default
        limiter = new TopicListMemoryLimiter(new CollectorRegistry(), "pulsar_broker_",
                OpenTelemetry.noop().getMeter("benchmark"), MAX_MEMORY, 10_000, 25_000, MAX_MEMORY, 10_000, 25_000);
    }

    @TearDown
    public void tearDown() {
        limiter.close();
    }

    @Benchmark
    @Threads(1)
    public long acquireAndRelease01Thread() {
        return acquireAndRelease();
    }

    @Benchmark
    @Threads(12)
    public long acquireAndRelease12Threads() {
        return acquireAndRelease();
    }

    private long acquireAndRelease() {
        AsyncDualMemoryLimiterPermit permit = limiter.acquire(1024, LimitType.DIRECT_MEMORY, NOT_CANCELLED).join();
        limiter.release(permit);
        return permit.getPermits();
    }
}
