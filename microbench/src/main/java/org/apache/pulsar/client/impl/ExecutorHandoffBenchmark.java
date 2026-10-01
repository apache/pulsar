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

import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.util.ExecutorProvider;
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
 * Measures concurrent submissions and completion on one worker. Each producer has at most one batch outstanding.
 * This is a component cost comparison, not a proof that these executors have interchangeable lifecycle contracts.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class ExecutorHandoffBenchmark {
    @Param({"jdk", "pulsar"})
    public String executorType;

    @Param({"16", "256"})
    public int tasksPerBatch;

    private ExecutorService executor;

    @Setup
    public void setup() {
        DefaultThreadFactory factory = new DefaultThreadFactory("executor-handoff");
        executor = switch (executorType) {
            case "jdk" -> Executors.newSingleThreadExecutor(factory);
            case "pulsar" -> new ExecutorProvider(1, "executor-handoff").getExecutor();
            default -> throw new IllegalArgumentException(executorType);
        };
    }

    @Benchmark
    public void submitAndComplete() throws InterruptedException {
        CountDownLatch completed = new CountDownLatch(tasksPerBatch);
        Runnable task = completed::countDown;
        for (int i = 0; i < tasksPerBatch; i++) {
            executor.execute(task);
        }
        if (!completed.await(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Executor did not complete the submitted batch");
        }
    }

    @TearDown
    public void tearDown() throws InterruptedException {
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        if (!executor.isTerminated()) {
            executor.shutdownNow();
            throw new IllegalStateException("Executor did not terminate after completed batches");
        }
    }
}
