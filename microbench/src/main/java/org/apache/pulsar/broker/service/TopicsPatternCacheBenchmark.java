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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.topics.TopicsPattern;
import org.apache.pulsar.common.topics.TopicsPatternFactory;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Compares compiling the topics pattern of a topic list request, which the broker did for the topic list watcher of
 * every pattern consumer and for every CommandGetTopicsOfNamespace with a pattern, with getting it from
 * {@link TopicsPatternCache}. Each operation gets the pattern of the next regex of a set of distinct regexes, one
 * per namespace, like the pattern consumers of an application with many namespaces. With {@code held}, the patterns
 * stay referenced after the operation, like the topic list watchers reference their patterns; otherwise only the
 * operation uses them, like a CommandGetTopicsOfNamespace, and the cache loses them in garbage collections. The 12
 * threads stand for the Netty IO threads of a broker with 6 CPUs, and each thread starts at a different regex.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 2, jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class TopicsPatternCacheBenchmark {
    private static final TopicsPattern.RegexImplementation IMPLEMENTATION =
            TopicsPattern.RegexImplementation.RE2J_WITH_JDK_FALLBACK;

    @Param({"1000", "100000"})
    public int regexCount;

    @Param({"true", "false"})
    public boolean held;

    private String[] regexes;
    private TopicsPattern[] heldPatterns;
    private TopicsPatternCache topicsPatternCache;

    @Setup(Level.Trial)
    public void setup() {
        regexes = new String[regexCount];
        for (int i = 0; i < regexCount; i++) {
            regexes[i] = "persistent://tenant/namespace-" + i + "/topic-" + i + "-.*";
        }
        heldPatterns = new TopicsPattern[regexCount];
        topicsPatternCache = new TopicsPatternCache();
    }

    @State(Scope.Thread)
    public static class ThreadState {
        private int next;

        @Setup(Level.Trial)
        public void setup(TopicsPatternCacheBenchmark benchmark) {
            next = ThreadLocalRandom.current().nextInt(benchmark.regexCount);
        }

        int nextIndex(int count) {
            int index = next;
            if (++next == count) {
                next = 0;
            }
            return index;
        }
    }

    @Benchmark
    @Threads(1)
    public TopicsPattern compile(ThreadState threadState) {
        return compileNext(threadState);
    }

    @Benchmark
    @Threads(1)
    public TopicsPattern cached(ThreadState threadState) {
        return getNext(threadState);
    }

    @Benchmark
    @Threads(12)
    public TopicsPattern compile12Threads(ThreadState threadState) {
        return compileNext(threadState);
    }

    @Benchmark
    @Threads(12)
    public TopicsPattern cached12Threads(ThreadState threadState) {
        return getNext(threadState);
    }

    private TopicsPattern compileNext(ThreadState threadState) {
        int index = threadState.nextIndex(regexCount);
        return hold(index, TopicsPatternFactory.create(regexes[index], IMPLEMENTATION));
    }

    private TopicsPattern getNext(ThreadState threadState) {
        int index = threadState.nextIndex(regexCount);
        return hold(index, topicsPatternCache.get(regexes[index], IMPLEMENTATION));
    }

    private TopicsPattern hold(int index, TopicsPattern topicsPattern) {
        if (held) {
            heldPatterns[index] = topicsPattern;
        }
        return topicsPattern;
    }
}
