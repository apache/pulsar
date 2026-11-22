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
package org.apache.pulsar.broker.service.persistent;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.time.Clock;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.qos.MonotonicClock;
import org.apache.pulsar.broker.service.BacklogQuotaManager;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.GetStatsOptions;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
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
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures the first stats snapshot after loading a topic containing historical messages, before any new writes.
 * The storage callback runs on another thread without simulated network delay, so results measure snapshot and
 * callback handoff costs rather than BookKeeper latency. Topic construction is outside the measured operation.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class TopicStatsLastPublishTimeBenchmark {
    @Param({"false", "true"})
    public boolean excludeLastPublishTime;

    private BrokerService brokerService;
    private ManagedLedger ledger;
    private MessageDeduplication deduplication;
    private GetStatsOptions options;
    private PersistentTopic topic;
    private ExecutorService storageCallbacks;
    private ByteBuf lastEntry;

    @Setup(Level.Trial)
    public void setup() {
        PulsarService pulsar = mock(PulsarService.class, withSettings().stubOnly());
        ServiceConfiguration config = new ServiceConfiguration();
        config.setClusterName("local");
        MonotonicClock monotonicClock = System::nanoTime;
        when(pulsar.getConfiguration()).thenReturn(config);
        when(pulsar.getMonotonicClock()).thenReturn(monotonicClock);
        when(pulsar.getBrokerId()).thenReturn("benchmark-broker");
        BacklogQuotaManager quotas = mock(BacklogQuotaManager.class, withSettings().stubOnly());
        when(quotas.getDefaultQuota()).thenReturn(BacklogQuotaImpl.builder().limitSize(-1).limitTime(-1).build());
        brokerService = mock(BrokerService.class, withSettings().stubOnly());
        when(brokerService.pulsar()).thenReturn(pulsar);
        when(brokerService.getPulsar()).thenReturn(pulsar);
        when(brokerService.getClock()).thenReturn(Clock.systemUTC());
        when(brokerService.getBacklogQuotaManager()).thenReturn(quotas);
        deduplication = mock(MessageDeduplication.class, withSettings().stubOnly());
        when(deduplication.getStatus()).thenReturn(MessageDeduplication.Status.Disabled);

        Position position = PositionFactory.create(1, 0);
        ledger = mock(ManagedLedger.class, withSettings().stubOnly());
        when(ledger.getLastConfirmedEntry()).thenReturn(position);
        TreeMap<Long, LedgerInfo> ledgers = new TreeMap<>();
        ledgers.put(1L, new LedgerInfo().setLedgerId(1).setEntries(1));
        when(ledger.getLedgersInfo()).thenReturn(ledgers);
        MessageMetadata metadata = new MessageMetadata().setProducerName("historical-producer")
                .setSequenceId(0).setPublishTime(1_000_000L);
        lastEntry = Unpooled.buffer(4 + metadata.getSerializedSize());
        lastEntry.writeInt(metadata.getSerializedSize());
        metadata.writeTo(lastEntry);
        storageCallbacks = Executors.newSingleThreadExecutor(new DefaultThreadFactory("stats-storage-callback"));
        doAnswer(invocation -> {
            ReadEntryCallback callback = invocation.getArgument(1);
            Object context = invocation.getArgument(2);
            storageCallbacks.execute(() -> callback.readEntryComplete(
                    EntryImpl.createWithRetainedDuplicate(position, lastEntry, 0), context));
            return null;
        }).when(ledger).asyncReadEntry(any(Position.class), any(ReadEntryCallback.class), any());
        options = GetStatsOptions.builder().excludeLastPublishTime(excludeLastPublishTime).build();
        loadTopic();
        TopicStatsImpl stats = firstSnapshot();
        long expectedTimestamp = excludeLastPublishTime ? 0 : 1_000_000L;
        if (stats == null || stats.lastPublishTimeStamp != expectedTimestamp) {
            throw new IllegalStateException("Stats fixture did not return the expected last publish time");
        }
    }

    @Setup(Level.Invocation)
    public void loadTopic() {
        // A fresh instance preserves the cold timestamp cache on every measured snapshot, without reflection.
        topic = new PersistentTopic("persistent://public/default/stats-benchmark",
                brokerService, ledger, deduplication);
    }

    @Benchmark
    public TopicStatsImpl firstSnapshot() {
        return topic.getStats(options);
    }

    @TearDown(Level.Trial)
    public void cleanup() throws InterruptedException {
        storageCallbacks.shutdown();
        try {
            if (!storageCallbacks.awaitTermination(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Storage callbacks did not finish");
            }
        } finally {
            lastEntry.release();
        }
    }
}
