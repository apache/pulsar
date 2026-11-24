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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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

/** Isolates selection; excludes broker setup, networking, and dispatch payload processing. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class ConsumerPrioritySelectorBenchmark {
    @Param({"1", "16", "50", "256"})
    public int consumerCount;

    @Param({"1", "4"})
    public int priorityLevels;

    @Param({"0", "3"})
    public int priorityOffset;

    @Param({"READY", "SPARSE", "LAST_LEVEL", "BLOCKED", "FLOW"})
    public String availability;

    private List<Slot> consumers;
    private ConsumerPrioritySelector<Slot> selector;
    private int cursor;
    private int invocations;

    @Setup
    public void setup() {
        consumers = new CopyOnWriteArrayList<>();
        for (int i = 0; i < consumerCount; i++) {
            int level = i * Math.min(priorityLevels, consumerCount) / consumerCount;
            Slot slot = new Slot(priorityOffset + level * 3);
            slot.permits = switch (availability) {
                case "READY", "FLOW" -> 32;
                case "SPARSE" -> i % 8 == 0 ? 32 : 0;
                case "LAST_LEVEL" -> level == Math.min(priorityLevels, consumerCount) - 1 ? 32 : 0;
                case "BLOCKED" -> 0;
                default -> throw new IllegalArgumentException(availability);
            };
            consumers.add(slot);
        }
        selector = new ConsumerPrioritySelector<>(consumers, c -> c.priority, c -> c.permits > 0);
        // Exercise wraparound even when no selection succeeds to advance the cursor.
        cursor = consumerCount / 2;
    }

    @Benchmark
    public int select() {
        if (cursor >= consumers.size()) {
            cursor = 0;
        }
        int selected = selector.select(cursor);
        if (selected >= 0) {
            cursor = selected + 1;
        }
        if (availability.equals("FLOW")) {
            if (selected >= 0) {
                consumers.get(selected).permits--;
            }
            // Periodic FLOW-like permit replenishment lets exhausted higher priorities become ready again.
            if (++invocations == consumerCount * 32) {
                for (Slot consumer : consumers) {
                    consumer.permits = 32;
                }
                invocations = 0;
            }
        }
        return selected;
    }

    private static final class Slot {
        private final int priority;
        private volatile int permits;

        private Slot(int priority) {
            this.priority = priority;
        }
    }
}
