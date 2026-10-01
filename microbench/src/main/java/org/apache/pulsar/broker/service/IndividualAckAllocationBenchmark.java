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
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.MessageIdData;
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

/**
 * Isolates list assembly for non-transactional grouped acknowledgements in Consumer.individualAck.
 * Both lists escape to model their use by asynchronous persistence and completion callbacks.
 * This excludes pending-ack lookups and persistence, and does not estimate full broker throughput.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class IndividualAckAllocationBenchmark {
    @Param({"1", "100", "1000", "10000"})
    public int messageCount;

    private CommandAck ack;

    @Setup
    public void setup() {
        ack = new CommandAck().setConsumerId(1).setAckType(CommandAck.AckType.Individual);
        for (int i = 0; i < messageCount; i++) {
            ack.addMessageId().setLedgerId(1).setEntryId(i);
        }
    }

    @Benchmark
    public void growingLists(Blackhole blackhole) {
        assemble(new ArrayList<>(), new ArrayList<>(), blackhole);
    }

    @Benchmark
    public void sizedLists(Blackhole blackhole) {
        assemble(new ArrayList<>(ack.getMessageIdsCount()),
                new ArrayList<>(ack.getMessageIdsCount()), blackhole);
    }

    private void assemble(List<Position> positions, List<Completion> completions, Blackhole blackhole) {
        for (int i = 0; i < ack.getMessageIdsCount(); i++) {
            MessageIdData id = ack.getMessageIdAt(i);
            Position position = PositionFactory.create(id.getLedgerId(), id.getEntryId());
            positions.add(position);
            completions.add(new Completion(null, position, false, 1));
        }
        blackhole.consume(positions);
        blackhole.consume(completions);
    }

    private record Completion(Consumer consumer, Position position, boolean hasAckSet, long ackedCount) {
    }
}
