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

import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
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

/** Models queued arrival/drain scheduling, excluding message decoding, listener calls and OS thread handoff. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class ListenerTaskSchedulingBenchmark {
    @Param({"1", "16", "1024"})
    public int notifications;

    @Param({"fresh", "reused", "coalesced"})
    public String scheduling;

    private QueuedExecutor executor;
    private ListenerTaskScheduler scheduler;
    private Runnable arrival;
    private Runnable trigger;
    private int incoming;
    private int received;

    @Setup
    public void setup() {
        executor = new QueuedExecutor();
        scheduler = new ListenerTaskScheduler(executor, this::drain);
        arrival = () -> incoming++;
        Runnable reusedDrain = this::drain;
        trigger = switch (scheduling) {
            case "fresh" -> () -> executor.execute(() -> drain());
            case "reused" -> () -> executor.execute(reusedDrain);
            case "coalesced" -> scheduler::trigger;
            default -> throw new IllegalArgumentException(scheduling);
        };
    }

    @Benchmark
    public int notifyAndDrain() {
        received = 0;
        for (int i = 0; i < notifications; i++) {
            executor.execute(arrival);
            trigger.run();
        }
        Runnable task;
        while ((task = executor.tasks.poll()) != null) {
            task.run();
        }
        if (received != notifications || incoming != 0) {
            throw new IllegalStateException("A notification was lost");
        }
        return received;
    }

    private void drain() {
        received += incoming;
        incoming = 0;
    }

    private static final class QueuedExecutor implements Executor {
        private final Queue<Runnable> tasks = new LinkedBlockingQueue<>();

        @Override
        public void execute(Runnable command) {
            tasks.add(command);
        }
    }
}
