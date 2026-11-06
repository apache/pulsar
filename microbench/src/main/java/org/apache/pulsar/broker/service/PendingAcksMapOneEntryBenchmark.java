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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * A consumer whose pending acks drop to none between messages, as on a topic with a low rate, with the real
 * pending-ACK map's locks and outer ledger map: each message is added, looked up when its ack arrives and removed,
 * which empties the map.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class PendingAcksMapOneEntryBenchmark {
    private PendingAcksMap pendingAcks;
    private long nextEntryId;

    @Setup
    public void setup() {
        Consumer owner = new Consumer("benchmark-owner", 0);
        pendingAcks = new PendingAcksMap(owner, () -> null, () -> null);
    }

    @Benchmark
    public int oneEntryAtATime() {
        long entryId = nextEntryId++;
        pendingAcks.addPendingAckIfAllowed(1, entryId, 1, 0);
        int remainingUnacked = pendingAcks.getRemainingUnacked(1, entryId);
        pendingAcks.removeAndGetRemainingUnacked(1, entryId);
        return remainingUnacked;
    }
}
