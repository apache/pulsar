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

import static org.mockito.Mockito.mock;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Range;
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

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
@State(Scope.Benchmark)
public class AutoSplitSelectorBenchmark {

    @Param({"2", "10", "50"})
    private int consumerCount;

    private HashRangeAutoSplitStickyKeyConsumerSelector selector;
    private ConcurrentSkipListMap<Integer, Consumer> previousLookup;

    @Setup
    public void setup() throws Exception {
        selector = new HashRangeAutoSplitStickyKeyConsumerSelector(true);
        for (int i = 0; i < consumerCount; i++) {
            selector.addConsumer(mock(Consumer.class)).join();
        }
        previousLookup = new ConcurrentSkipListMap<>();
        for (Map.Entry<Consumer, java.util.List<Range>> entry : selector.getConsumerKeyHashRanges().entrySet()) {
            for (Range range : entry.getValue()) {
                previousLookup.put(range.getEnd(), entry.getKey());
            }
        }
    }

    @Benchmark
    public Consumer skipListLookup(Cursor cursor) {
        return previousLookup.ceilingEntry(cursor.nextHash()).getValue();
    }

    @Benchmark
    public Consumer snapshotLookup(Cursor cursor) {
        return selector.select(cursor.nextHash());
    }

    @State(Scope.Thread)
    public static class Cursor {
        private int hash;

        int nextHash() {
            hash = (hash + 40503) & 0xffff;
            return hash;
        }
    }
}
