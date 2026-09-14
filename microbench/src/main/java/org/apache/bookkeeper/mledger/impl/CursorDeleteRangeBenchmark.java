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
package org.apache.bookkeeper.mledger.impl;

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

/**
 * Compares temporary-position versus primitive-bound assembly for the actual cursor range set.
 * The isolated temporary position may be escape-eliminated; this does not reproduce the full asyncDelete JIT context.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class CursorDeleteRangeBenchmark {
    @Param({"1", "100", "1000"})
    public int messageCount;

    @Param({"false", "true"})
    public boolean primitiveBounds;

    private Position[] positions;
    private PositionRangeSet ranges;

    @Setup
    public void setup() {
        ranges = new PositionRangeSet(PositionFactory::create, false);
        positions = new Position[messageCount];
        for (int i = 0; i < messageCount; i++) {
            positions[i] = PositionFactory.create(1, i);
        }
    }

    @Benchmark
    public int assembleRanges() {
        ranges.clear();
        for (Position position : positions) {
            if (primitiveBounds) {
                ranges.addOpenClosed(position.getLedgerId(), position.getEntryId() - 1,
                        position.getLedgerId(), position.getEntryId());
            } else {
                Position previous = PositionFactory.create(position.getLedgerId(), position.getEntryId() - 1);
                ranges.addOpenClosed(previous.getLedgerId(), previous.getEntryId(),
                        position.getLedgerId(), position.getEntryId());
            }
        }
        return ranges.size();
    }
}
