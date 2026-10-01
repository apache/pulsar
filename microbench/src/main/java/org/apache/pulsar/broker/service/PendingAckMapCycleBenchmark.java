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

/** Includes the real pending-ACK map's locks, outer ledger lookup and packed value handling, without contention. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class PendingAckMapCycleBenchmark {
    @Param({"1024", "65536"})
    public int windowSize;

    @Param({"false", "true"})
    public boolean shuffled;

    private PendingAcksMap pendingAcks;
    private long[] entryIds;
    private long nextEntryId;
    private int cursor;

    @Setup
    public void setup() {
        Consumer owner = new Consumer("benchmark-owner", 0);
        pendingAcks = new PendingAcksMap(owner, () -> null, () -> null);
        entryIds = new long[windowSize];
        for (int i = 0; i < windowSize; i++) {
            entryIds[i] = i;
            pendingAcks.addPendingAckIfAllowed(1, i, 1, 0);
        }
        nextEntryId = windowSize;
    }

    @Benchmark
    public int rotateWindow() {
        int next = cursor++;
        int index = (shuffled ? next * 4051 : next) & (windowSize - 1);
        long entryId = entryIds[index];
        int count = pendingAcks.getRemainingUnacked(1, entryId);
        pendingAcks.remove(1, entryId);
        long addedEntryId = nextEntryId++;
        pendingAcks.addPendingAckIfAllowed(1, addedEntryId, 1, 0);
        entryIds[index] = addedEntryId;
        return count;
    }
}
