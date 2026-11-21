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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.AckSetStateUtil;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.metadata.bookkeeper.BKCluster;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Compares isolated map lookup strategies and the production cursor/dispatch paths with cached entries.
 * RealState starts a broker and bookie and establishes ordinary and pending batch acknowledgments in trial setup.
 * The measured methods do no network or storage IO; they include the cursor class guard, BitSet snapshot copying,
 * and, for dispatch, pending-ack lookup and mask combination. These are not end-to-end throughput measurements.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class DispatchPositionBenchmark {
    private static final int ENTRY_COUNT = 10000;

    @State(Scope.Thread)
    public static class LookupState {
        @Param({"0", "128", "10000"})
        public int acknowledgedEntries;

        private Entry[] entries;
        private ConcurrentSkipListMap<Position, long[]> ackSets;
        private int index;

        @Setup
        public void setup() {
            entries = new Entry[ENTRY_COUNT];
            ackSets = new ConcurrentSkipListMap<>();
            MessageMetadata metadata = new MessageMetadata().setProducerName("producer").setSequenceId(1)
                    .setPublishTime(1);
            for (int i = 0; i < ENTRY_COUNT; i++) {
                EntryImpl entry = EntryImpl.create(i / 1000, i % 1000, new byte[0]);
                Position position = entry.getPosition();
                entries[i] = EntryAndMetadata.create(entry, metadata);
                if (i < acknowledgedEntries) {
                    ackSets.put(position, new long[] {1});
                }
            }
        }

        private Entry nextEntry() {
            Entry entry = entries[index++];
            if (index == entries.length) {
                index = 0;
            }
            return entry;
        }

        @TearDown
        public void tearDown() {
            for (Entry entry : entries) {
                entry.release();
            }
        }
    }

    @Benchmark
    public long[] recreatedPosition(LookupState state) {
        Entry entry = state.nextEntry();
        return state.ackSets.get(PositionFactory.create(entry.getLedgerId(), entry.getEntryId()));
    }

    @Benchmark
    public long[] cachedPosition(LookupState state) {
        return state.ackSets.get(state.nextEntry().getPosition());
    }

    @Benchmark
    public long[] skipPositionForEmptyMap(LookupState state) {
        Entry entry = state.nextEntry();
        if (state.ackSets.isEmpty()) {
            return null;
        }
        return state.ackSets.get(PositionFactory.create(entry.getLedgerId(), entry.getEntryId()));
    }

    @Benchmark
    public long[] realCursorByPosition(RealState state) {
        Entry entry = state.nextEntry();
        return state.cursor.getDeletedBatchIndexesAsLongArray(
                PositionFactory.create(entry.getLedgerId(), entry.getEntryId()));
    }

    @Benchmark
    public long[] realCursorByIds(RealState state) {
        Entry entry = state.nextEntry();
        return state.cursor.getDeletedBatchIndexesAsLongArray(entry.getLedgerId(), entry.getEntryId());
    }

    @Benchmark
    public void realDispatch(RealState state, Blackhole blackhole) {
        state.dispatchEntries.set(0, state.nextEntry());
        EntryBatchIndexesAcks indexesAcks = EntryBatchIndexesAcks.get(1);
        try {
            blackhole.consume(state.dispatcher.filterEntriesForConsumer(state.dispatchEntries, state.batchSizes,
                    SendMessageInfo.getThreadLocal(), indexesAcks, state.cursor, true, null));
            blackhole.consume(indexesAcks.getAckSet(0));
        } finally {
            indexesAcks.recycle();
        }
    }

    @State(Scope.Thread)
    public static class RealState {
        @Param({"0", "128", "10000"})
        public int acknowledgedEntries;

        @Param({"false", "true"})
        public boolean transactionsEnabled;

        private BKCluster bookies;
        private PulsarService pulsar;
        private ManagedCursor cursor;
        private PersistentDispatcherMultipleConsumers dispatcher;
        private Entry[] entries;
        private final List<Entry> dispatchEntries = new ArrayList<>(1);
        private EntryBatchSizes batchSizes;
        private int index;

        @Setup
        public void setup() throws Exception {
            try {
                startBroker();
                PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService()
                        .getTopic("persistent://benchmark/ns/dispatch", true).get(30, TimeUnit.SECONDS).orElseThrow();
                PersistentSubscription subscription = (PersistentSubscription) topic.createSubscription(
                        "cursor", InitialPosition.Earliest, false, Map.of()).get(30, TimeUnit.SECONDS);
                cursor = subscription.getCursor();
                // A subclass would bypass the empty-map shortcut that this benchmark is meant to measure.
                if (cursor.getClass() != ManagedCursorImpl.class) {
                    throw new IllegalStateException("Expected an exact ManagedCursorImpl instance");
                }
                dispatcher = new PersistentDispatcherMultipleConsumers(topic, cursor, subscription);
                populateEntries(topic, subscription);
                dispatchEntries.add(null);
                batchSizes = EntryBatchSizes.get(1);
                verifyLookup(0, acknowledgedEntries > 0);
                verifyLookup(ENTRY_COUNT - 1, acknowledgedEntries == ENTRY_COUNT);
            } catch (Exception e) {
                try {
                    tearDown();
                } catch (Exception cleanupFailure) {
                    e.addSuppressed(cleanupFailure);
                }
                throw e;
            }
        }

        private void startBroker() throws Exception {
            String metadataUrl = "memory:dispatch-benchmark-" + UUID.randomUUID();
            bookies = BKCluster.builder().metadataServiceUri(metadataUrl).numBookies(1).build();
            ServiceConfiguration config = new ServiceConfiguration();
            config.setMetadataStoreUrl(metadataUrl);
            config.setConfigurationMetadataStoreUrl(metadataUrl);
            config.setClusterName("benchmark");
            config.setAdvertisedAddress("localhost");
            config.setBrokerServicePort(Optional.of(0));
            config.setWebServicePort(Optional.of(0));
            config.setManagedLedgerDefaultEnsembleSize(1);
            config.setManagedLedgerDefaultWriteQuorum(1);
            config.setManagedLedgerDefaultAckQuorum(1);
            config.setDefaultNumberOfNamespaceBundles(1);
            config.setLoadBalancerEnabled(false);
            config.setBrokerDeleteInactiveTopicsEnabled(false);
            config.setTransactionCoordinatorEnabled(transactionsEnabled);
            config.setBrokerShutdownTimeoutMs(0L);
            config.setNumIOThreads(2);
            config.setNumExecutorThreadPoolSize(2);
            config.setNumOrderedExecutorThreads(2);
            config.setNumHttpServerThreads(4);
            config.setBookkeeperClientNumWorkerThreads(2);
            config.setBookkeeperClientNumIoThreads(2);
            config.setManagedLedgerNumSchedulerThreads(2);
            config.setManagedLedgerCacheSizeMB(8);
            config.setTopicOrderedExecutorThreadNum(2);
            pulsar = new PulsarService(config);
            pulsar.start();
            try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsar.getWebServiceAddress()).build()) {
                admin.clusters().createCluster("benchmark", ClusterData.builder()
                        .serviceUrl(pulsar.getWebServiceAddress())
                        .brokerServiceUrl(pulsar.getBrokerServiceUrl()).build());
                admin.tenants().createTenant("benchmark", TenantInfo.builder()
                        .allowedClusters(Set.of("benchmark")).build());
                admin.namespaces().createNamespace("benchmark/ns", Set.of("benchmark"));
            }
        }

        private void populateEntries(PersistentTopic topic, PersistentSubscription subscription) throws Exception {
            entries = new Entry[ENTRY_COUNT];
            MessageMetadata metadata = new MessageMetadata().setProducerName("producer").setSequenceId(1)
                    .setPublishTime(1).setNumMessagesInBatch(3);
            // Dispatch only reads the entry metadata, not the individual message payloads.
            ByteBuf data = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c,
                    metadata, Unpooled.EMPTY_BUFFER);
            try {
                byte[] bytes = new byte[data.readableBytes()];
                data.getBytes(data.readerIndex(), bytes);
                List<Position> ordinaryAcks = new ArrayList<>(acknowledgedEntries);
                List<MutablePair<Position, Integer>> pendingAcks = new ArrayList<>(acknowledgedEntries);
                for (int i = 0; i < ENTRY_COUNT; i++) {
                    Position position = topic.getManagedLedger().addEntry(bytes, 3);
                    entries[i] = EntryAndMetadata.create(EntryImpl.create(position, data, 0), metadata);
                    if (i < acknowledgedEntries) {
                        // Acknowledge index 0 normally, and index 1 in a pending transaction.
                        // Index 2 remains deliverable, so repeated filtering never releases the entry.
                        ordinaryAcks.add(AckSetStateUtil.createPositionWithAckSet(
                                position.getLedgerId(), position.getEntryId(), new long[] {6}));
                        if (transactionsEnabled) {
                            pendingAcks.add(MutablePair.of(AckSetStateUtil.createPositionWithAckSet(
                                    position.getLedgerId(), position.getEntryId(), new long[] {5}), 3));
                        }
                    }
                }
                if (!ordinaryAcks.isEmpty()) {
                    cursor.delete(ordinaryAcks);
                }
                if (!pendingAcks.isEmpty()) {
                    subscription.transactionIndividualAcknowledge(new TxnID(0, 1), pendingAcks)
                            .get(30, TimeUnit.SECONDS);
                }
            } finally {
                data.release();
            }
        }

        private void verifyLookup(int entryIndex, boolean acknowledged) {
            Entry entry = entries[entryIndex];
            long[] expectedCursor = acknowledged ? new long[] {6} : null;
            if (!Arrays.equals(cursor.getDeletedBatchIndexesAsLongArray(entry.getPosition()), expectedCursor)
                    || !Arrays.equals(cursor.getDeletedBatchIndexesAsLongArray(
                            entry.getLedgerId(), entry.getEntryId()), expectedCursor)) {
                throw new IllegalStateException("Cursor fixture has unexpected batch acknowledgments");
            }
            dispatchEntries.set(0, entry);
            EntryBatchIndexesAcks indexesAcks = EntryBatchIndexesAcks.get(1);
            try {
                int count = dispatcher.filterEntriesForConsumer(dispatchEntries, batchSizes,
                        SendMessageInfo.getThreadLocal(), indexesAcks, cursor, true, null);
                long[] expectedDispatch = acknowledged ? new long[] {transactionsEnabled ? 4 : 6} : null;
                if (count != 1 || !Arrays.equals(indexesAcks.getAckSet(0), expectedDispatch)) {
                    throw new IllegalStateException("Dispatch fixture has unexpected pending acknowledgments");
                }
            } finally {
                indexesAcks.recycle();
            }
        }

        private Entry nextEntry() {
            Entry entry = entries[index++];
            if (index == entries.length) {
                index = 0;
            }
            return entry;
        }

        @TearDown
        public void tearDown() throws Exception {
            if (entries != null) {
                for (Entry entry : entries) {
                    if (entry != null) {
                        entry.release();
                    }
                }
                entries = null;
            }
            if (batchSizes != null) {
                batchSizes.recyle();
                batchSizes = null;
            }
            try {
                if (dispatcher != null) {
                    dispatcher.close().get(30, TimeUnit.SECONDS);
                    dispatcher = null;
                }
            } finally {
                try {
                    if (pulsar != null) {
                        pulsar.close();
                        pulsar = null;
                    }
                } finally {
                    if (bookies != null) {
                        bookies.close();
                        bookies = null;
                    }
                }
            }
        }
    }
}
