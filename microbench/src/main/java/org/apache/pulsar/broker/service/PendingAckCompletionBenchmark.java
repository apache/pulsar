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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
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
import org.openjdk.jmh.infra.Blackhole;

/** Compares the previous record-list layout with the actual command-local completion snapshot. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class PendingAckCompletionBenchmark {
    @Param({"1", "100", "1000"})
    public int entries;

    @Param({"false", "true"})
    public boolean batched;

    @Param({"records", "arrays"})
    public String representation;

    private Position[] positions;
    private Consumer owner;
    private volatile Object retained;

    @Setup
    public void setup() {
        owner = new Consumer("benchmark", 1000);
        positions = new Position[entries];
        for (int i = 0; i < entries; i++) {
            positions[i] = PositionFactory.create(123, i);
        }
    }

    @Benchmark
    public long buildAndRead(Blackhole blackhole) {
        long result = 0;
        if ("records".equals(representation)) {
            List<LegacyCompletion> completions = new ArrayList<>(entries);
            for (int i = 0; i < entries; i++) {
                completions.add(new LegacyCompletion(owner, positions[i], batched, i));
            }
            retained = completions;
            for (LegacyCompletion completion : completions) {
                blackhole.consume(completion.consumer());
                result += completion.position().getEntryId();
                if (completion.hasAckSet()) {
                    result += Math.max(0, completion.ackedCount());
                }
            }
        } else {
            Consumer.PendingAckCompletions completions = new Consumer.PendingAckCompletions(entries);
            for (int i = 0; i < entries; i++) {
                completions.add(owner, positions[i], batched, i);
            }
            retained = completions;
            for (int i = 0; i < completions.size(); i++) {
                blackhole.consume(completions.consumerAt(i));
                result += completions.positionAt(i).getEntryId();
                if (completions.hasAckSetAt(i)) {
                    result += Math.max(0, completions.ackedCountAt(i));
                }
            }
        }
        return result;
    }

    // Previous production record layout. Positions are prebuilt and owner calls are excluded.
    private record LegacyCompletion(Consumer consumer, Position position, boolean hasAckSet, long ackedCount) {
    }
}
