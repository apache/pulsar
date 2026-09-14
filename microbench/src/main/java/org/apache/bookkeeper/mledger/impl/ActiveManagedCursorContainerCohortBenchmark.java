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
import org.apache.bookkeeper.mledger.PositionFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class ActiveManagedCursorContainerCohortBenchmark {
    @Param({"100", "500"})
    public int numberOfCursors;
    private ActiveManagedCursorContainerImpl container;
    private MockManagedCursor[] cohort;

    @Setup(Level.Trial)
    public void setup() {
        container = new ActiveManagedCursorContainerImpl();
        cohort = new MockManagedCursor[32];
        for (int i = 0; i < numberOfCursors; i++) {
            var position = PositionFactory.create(1, i);
            var cursor = MockManagedCursor.createCursor(container, "cursor" + i, position);
            container.add(cursor, position);
            if (i >= numberOfCursors - cohort.length) {
                cohort[i - (numberOfCursors - cohort.length)] = cursor;
            }
        }
        container.checkOrderingAndNumberOfCursorsState();
    }

    @Benchmark
    @Threads(1)
    @OperationsPerInvocation(32)
    public int advanceCohort() {
        // Queue a wave in ascending old-position order. Each cursor advances beyond all old cohort positions.
        for (var cursor : cohort) {
            cursor.seek(cursor.getReadPosition().getPositionAfterEntries(cohort.length));
        }
        // 32 pending updates stay below trackedNodeCount / 2 for both parameters, forcing incremental processing.
        return container.getNumberOfCursorsAtSamePositionOrBefore(cohort[cohort.length - 1]);
    }

    @TearDown(Level.Trial)
    public void verify() {
        container.checkOrderingAndNumberOfCursorsState();
        for (int i = 0; i < cohort.length; i++) {
            int expected = numberOfCursors - cohort.length + i + 1;
            if (container.getNumberOfCursorsAtSamePositionOrBefore(cohort[i]) != expected) {
                throw new AssertionError("Wrong cohort rank at index " + i);
            }
        }
    }
}
