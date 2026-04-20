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
import org.apache.pulsar.common.naming.TopicName;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * JMH benchmark for {@link TopicName#get(String)} cold-start (cache-miss) performance.
 *
 * <p>Each invocation calls {@code TopicName.get()} 1,000,000 times with distinct topic
 * strings, forcing every call to go through the full parse-and-construct path.
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :microbench:shadowJar
 *   java -jar microbench/build/libs/microbench-*-benchmarks.jar TopicNameGetBenchmark
 * </pre>
 */
@Fork(3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@State(Scope.Thread)
public class TopicNameGetBenchmark {

    private static final int TOPIC_COUNT = 1_000_000;

    private String[] topics;

    @Setup(Level.Invocation)
    public void prepare() {
        // Pre-generate 1M distinct topic strings (string-concat cost excluded from measurement).
        topics = new String[TOPIC_COUNT];
        for (int i = 0; i < TOPIC_COUNT; i++) {
            topics[i] = "persistent://public/default/topic-" + i;
        }
        // Clear cache to ensure a cold start.
        TopicName.clearIfReachedMaxCapacity(0);
    }

    @Benchmark
    public void coldStartGet(Blackhole bh) {
        for (int i = 0; i < TOPIC_COUNT; i++) {
            bh.consume(TopicName.get(topics[i]));
        }
    }
}
