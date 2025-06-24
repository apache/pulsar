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

package org.apache.pulsar.common.naming;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmark TopicName.get performance.
 */
@Fork(3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class TopicNameBenchmark {
    @State(Scope.Thread)
    public static class TestState {
        @Param({"false", "true"})
        private boolean invalidateCache;
        private long counter = 0;
        private String[] topicNames;

        @Setup(Level.Trial)
        public void setup() {
            topicNames = new String[100000];
            for (int i = 0; i < topicNames.length; i++) {
                topicNames[i] = String.format("persistent://tenant-%d/ns-%d/topic-%d", i % 100, i % 1000, i);
            }
        }

        @TearDown(Level.Iteration)
        public void tearDown() {
            if (invalidateCache) {
                TopicName.invalidateCache();
                NamespaceName.invalidateCache();
            }
            counter = 0;
        }

        public String getNextTopicName() {
            String topicName = topicNames[(int) (counter++ % topicNames.length)];
            return topicName;
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Measurement(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
    @Threads(1)
    public TopicName topicLookup001(TestState state) {
        return TopicName.get(state.getNextTopicName());
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Measurement(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
    @Threads(10)
    public TopicName topicLookup010(TestState state) {
        return TopicName.get(state.getNextTopicName());
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Measurement(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
    @Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
    @Threads(100)
    public TopicName topicLookup100(TestState state) {
        return TopicName.get(state.getNextTopicName());
    }
}
