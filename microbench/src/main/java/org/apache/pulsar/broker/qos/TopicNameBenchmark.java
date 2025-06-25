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
import org.apache.pulsar.common.naming.TopicName;
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

@Fork(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class TopicNameBenchmark {

    private static final String[] topicBases = {"test",
            "tenant/ns/test",
            "persistent://another-tenant/another-ns/test"
    };

    @Threads(1)
    @Benchmark
    @Warmup(time = 5, iterations = 1)
    @Measurement(time = 5, iterations = 1)
    public void testReadFromCache(Blackhole blackhole) {
        for (int i = 0; i < 10000; i++) {
            for (final var topicBase : topicBases) {
                blackhole.consume(TopicName.get(topicBase + i));
            }
        }
    }

    @Threads(1)
    @Benchmark
    @Warmup(time = 5, iterations = 1)
    @Measurement(time = 5, iterations = 1)
    public void testConstruct(Blackhole blackhole) {
        for (int i = 0; i < 10000; i++) {
            for (final var topicBase : topicBases) {
                blackhole.consume(new TopicName(topicBase + i, false));
            }
        }
    }

    @Threads(1)
    @Benchmark
    @Warmup(time = 5, iterations = 1)
    @Measurement(time = 5, iterations = 1)
    public void testConstructWithNamespaceInitialized(Blackhole blackhole) {
        for (int i = 0; i < 10000; i++) {
            for (final var topicBase : topicBases) {
                blackhole.consume(new TopicName(topicBase + i, true));
            }
        }
    }
}
