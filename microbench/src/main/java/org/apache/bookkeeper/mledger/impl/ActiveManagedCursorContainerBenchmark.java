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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.PositionFactory;
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

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
@Measurement(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
@Fork(1)
public class ActiveManagedCursorContainerBenchmark {
    @Param({"10", "100", "500"})
    private int numberOfCursors;

    public enum ActiveManagedCursorContainerImplType {
        DEFAULT(ActiveManagedCursorContainerImpl::new, true),
        NAVIGABLE_SET(ActiveManagedCursorContainerNavigableSetImpl::new, true),
        MANAGED_CURSOR_CONTAINER(ManagedCursorContainerImpl::new, false);

        private final Supplier<ActiveManagedCursorContainer> factory;
        private final boolean supportsGetNumberOfCursorsAtSamePositionOrBefore;

        ActiveManagedCursorContainerImplType(Supplier<ActiveManagedCursorContainer> factory,
                                             boolean supportsGetNumberOfCursorsAtSamePositionOrBefore) {
            this.factory = factory;
            this.supportsGetNumberOfCursorsAtSamePositionOrBefore = supportsGetNumberOfCursorsAtSamePositionOrBefore;
        }

        public ActiveManagedCursorContainer create() {
            return factory.get();
        }
    }
    @Param
    private ActiveManagedCursorContainerImplType activeManagedCursorContainerImplType;

    @Param({"0", "1", "10", "100"})
    private int getNumberOfCursorsAtSamePositionOrBeforeRatio;

    private ActiveManagedCursorContainer container;
    private final List<MockManagedCursor> cursors = new ArrayList<>();

    @State(Scope.Thread)
    public static class ThreadState {
        // Use a fixed seed for reproducibility in benchmarks
        Random random = new Random(1);
        long counter = 0;

        public long nextCounter() {
            return counter++;
        }
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        container = activeManagedCursorContainerImplType.create();
        IntStream.rangeClosed(1, numberOfCursors).mapToObj(
                        idx -> MockManagedCursor.createCursor(container, "cursor" + idx, PositionFactory.create(0,
                                idx)))
                .forEach(cursors::add);
        cursors.forEach(cursor -> container.add(cursor, cursor.getReadPosition()));
        checkState(container);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        // check that the state of the container is correct
        checkState(container);
    }

    private void checkState(ActiveManagedCursorContainer container) {
        if (container instanceof ActiveManagedCursorContainerImpl c) {
            c.checkOrderingAndNumberOfCursorsState();
        } else if (container instanceof ActiveManagedCursorContainerNavigableSetImpl c) {
            c.checkOrderingAndNumberOfCursorsState();
        }
    }

    @Threads(1)
    @Benchmark
    public int randomSeekingForward01(ThreadState threadState) {
        return doRandomSeekingForward(threadState);
    }

    @Threads(10)
    @Benchmark
    public int randomSeekingForward10(ThreadState threadState) {
        return doRandomSeekingForward(threadState);
    }

    private int doRandomSeekingForward(ThreadState threadState) {
        Random random = threadState.random;
        long counter = threadState.nextCounter();
        // pick a random cursor
        ManagedCursor cursor = cursors.get(random.nextInt(0, cursors.size()));
        // seek forward by a random number of entries
        cursor.seek(cursor.getReadPosition().getPositionAfterEntries(random.nextInt(1, 100)));
        // return the number of cursors at the same position or before so that this is also part of the benchmark
        return getNumberOfCursorsAtSamePositionOrBeforeRatio > 0
                && counter % getNumberOfCursorsAtSamePositionOrBeforeRatio == 0
                && activeManagedCursorContainerImplType.supportsGetNumberOfCursorsAtSamePositionOrBefore
                ? container.getNumberOfCursorsAtSamePositionOrBefore(cursor)
                // if benchmarking this method is not enabled or the implementation does not support it,
                // we return the entry id of the cursor's read position
                : (int) cursor.getReadPosition().getEntryId();
    }
}