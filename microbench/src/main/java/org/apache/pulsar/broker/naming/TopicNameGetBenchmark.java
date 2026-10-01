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
package org.apache.pulsar.broker.naming;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.common.naming.TopicName;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * JMH benchmark for {@link TopicName#get(String)} cold-start (cache-miss) performance
 * under 50-thread contention.
 *
 * <p>Uses {@code Mode.SingleShotTime} with {@code @Fork(10)} to measure
 * the total time of a fixed batch of cold-start calls. No cache clearing is
 * needed — each fork is a fresh JVM with an empty cache, and the batch size
 * is bounded to avoid OOM.
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :microbench:shadowJar
 *   java -jar microbench/build/libs/microbench-*-benchmarks.jar TopicNameGetBenchmark
 * </pre>
 */
@Fork(10)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@Threads(50)
@State(Scope.Thread)
public class TopicNameGetBenchmark {

    /**
     * Each thread processes 10,000 unique topics per invocation.
     * 50 threads × 10,000 = 500,000 total entries per invocation — well within memory.
     */
    private static final int BATCH_SIZE = 10_000;
    private static final AtomicInteger COUNTER = new AtomicInteger();

    private String[] topics;

    @Setup(Level.Invocation)
    public void prepare() {
        int base = COUNTER.getAndAdd(BATCH_SIZE);
        topics = new String[BATCH_SIZE];
        for (int i = 0; i < BATCH_SIZE; i++) {
            topics[i] = "persistent://public/default/topic-" + (base + i);
        }
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void coldStartGet(Blackhole bh) {
        for (int i = 0; i < BATCH_SIZE; i++) {
            bh.consume(TopicName.get(topics[i]));
        }
    }
}
