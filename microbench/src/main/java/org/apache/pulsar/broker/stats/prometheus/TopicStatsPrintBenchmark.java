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
package org.apache.pulsar.broker.stats.prometheus;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.policies.data.stats.TopicMetricBean;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Prints a batch of topic snapshots using the same metric streams as a broker scrape. Run with {@code -prof gc}
 * to compare bytes allocated per topic. The benchmark includes metric stream creation, grouping, flushing and release.
 * Trial setup builds the snapshots and allocates the reusable output buffer.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class TopicStatsPrintBenchmark {
    private static final int TOPICS_PER_SCRAPE = 64;
    private static final String CLUSTER = "benchmark-cluster";
    private static final String NAMESPACE = "benchmark-tenant/benchmark-namespace";

    private static final int MESSAGE_BYTES = 1024;

    private record Snapshot(String topic, TopicStats stats) {
    }

    @State(Scope.Thread)
    public abstract static class ScrapeState {
        private final Snapshot[] snapshots = new Snapshot[TOPICS_PER_SCRAPE];
        private final ByteBuf outputBuffer = Unpooled.buffer(4 * 1024 * 1024);
        private final SimpleTextOutputStream output = new SimpleTextOutputStream(outputBuffer);

        void setSnapshot(int index, String topic, TopicStats stats) {
            snapshots[index] = new Snapshot(topic, stats);
        }

        void useSharedStats(TopicStats stats) {
            for (int i = 0; i < snapshots.length; i++) {
                setSnapshot(i, topicName(i), stats);
            }
        }

        int print(boolean splitPartitionLabel, boolean exposeBacklogAge, String[] customLabels) {
            outputBuffer.clear();
            PrometheusMetricStreams streams = new PrometheusMetricStreams();
            try {
                for (Snapshot snapshot : snapshots) {
                    TopicStats.printTopicStats(streams, snapshot.stats(), Optional.empty(), CLUSTER, NAMESPACE,
                            snapshot.topic(), splitPartitionLabel, exposeBacklogAge, customLabels);
                }
                streams.flushAllToStream(output);
                return outputBuffer.readableBytes();
            } finally {
                streams.releaseAll();
            }
        }

        @TearDown
        public void cleanup() {
            outputBuffer.release();
        }
    }

    @State(Scope.Thread)
    public static class TopicMetrics extends ScrapeState {
        @Param({"false", "true"})
        public boolean splitTopicAndPartitionIndexLabel;

        /**
         * When enabled, each topic has one producer, one subscription, two consumers and one remote cluster.
         * When disabled, the topic has no entities, including subscriptions and remote clusters.
         */
        @Param({"false", "true"})
        public boolean includeEntities;

        private final String[] customLabels = {"environment", "production", "region", "us-east"};

        @Setup
        public void setup() {
            TopicStats stats = smallTopicStats();
            if (includeEntities) {
                populateSmallTopic(stats, true, true);
            }
            useSharedStats(stats);
        }
    }

    public enum AggregateFixture {
        LOCAL_AGGREGATES, GEO_AGGREGATES
    }

    /** Keep subscription and optional replication aggregates with producer and consumer detail disabled. */
    @State(Scope.Thread)
    public static class AggregateMetrics extends ScrapeState {
        @Param({"false", "true"})
        public boolean splitTopicAndPartitionIndexLabel;

        @Param
        public AggregateFixture fixture;

        @Setup
        public void setup() {
            TopicStats stats = smallTopicStats();
            populateSmallTopic(stats, false, fixture == AggregateFixture.GEO_AGGREGATES);
            useSharedStats(stats);
        }
    }

    public enum Detail {
        NONE, PRODUCERS, CONSUMERS, BOTH
    }

    /**
     * Each physical partition has 100 producers and 40 consumers on one or 40 subscriptions.
     * Producer and consumer detail can be enabled independently.
     */
    @State(Scope.Thread)
    public static class HighCardinalityMetrics extends ScrapeState {
        private static final int PRODUCERS = 100;
        private static final int CONSUMERS = 40;
        private static final int UNACKED_PER_CONSUMER = 12;

        @Param({"1", "40"})
        public int subscriptionCount;

        @Param
        public Detail detail;

        @Setup
        public void setup() {
            if (subscriptionCount != 1 && subscriptionCount != CONSUMERS) {
                throw new IllegalArgumentException("subscriptionCount must be 1 or " + CONSUMERS);
            }
            ConsumerFactory consumers = detail == Detail.CONSUMERS || detail == Detail.BOTH
                    ? new ConsumerFactory() : null;
            for (int i = 0; i < TOPICS_PER_SCRAPE; i++) {
                String topic = topicName(i);
                TopicStats stats = createTopicStats();
                if (detail == Detail.PRODUCERS || detail == Detail.BOTH) {
                    for (int p = 0; p < PRODUCERS; p++) {
                        int id = i * PRODUCERS + p;
                        addProducer(stats, "orders-publisher-" + id, 10_000 + id);
                    }
                }
                for (int s = 0; s < subscriptionCount; s++) {
                    String name = "orders-subscription-" + s;
                    AggregatedSubscriptionStats sub = addSubscription(stats, name,
                            stats.msgOutCounter / subscriptionCount);
                    sub.unackedMessages = UNACKED_PER_CONSUMER * sub.consumersCount;
                    if (consumers != null) {
                        for (int c = 0; c < sub.consumersCount; c++) {
                            long id = i * CONSUMERS + s * sub.consumersCount + c;
                            Consumer consumer = consumers.create(name, SubType.Shared, topic,
                                    20_000 + id, "orders-worker-" + id);
                            addConsumer(sub, consumer, UNACKED_PER_CONSUMER);
                        }
                    }
                }
                setSnapshot(i, topic, stats);
            }
        }

        private TopicStats createTopicStats() {
            TopicStats stats = new TopicStats();
            stats.producersCount = PRODUCERS;
            stats.consumersCount = CONSUMERS;
            stats.subscriptionsCount = subscriptionCount;
            stats.rateIn = PRODUCERS * 20;
            stats.rateOut = CONSUMERS * 48;
            stats.throughputIn = stats.rateIn * MESSAGE_BYTES;
            stats.throughputOut = stats.rateOut * MESSAGE_BYTES;
            stats.averageMsgSize = MESSAGE_BYTES;
            stats.msgInCounter = 10_000_000;
            stats.bytesInCounter = stats.msgInCounter * MESSAGE_BYTES;
            stats.msgOutCounter = CONSUMERS * 240_000L;
            stats.bytesOutCounter = stats.msgOutCounter * MESSAGE_BYTES;
            stats.msgBacklog = CONSUMERS * 51;
            populateStorageStats(stats);
            return stats;
        }
    }

    @Benchmark
    @OperationsPerInvocation(TOPICS_PER_SCRAPE)
    public int printScrape(TopicMetrics state) {
        return state.print(state.splitTopicAndPartitionIndexLabel, true, state.customLabels);
    }

    @Benchmark
    @OperationsPerInvocation(TOPICS_PER_SCRAPE)
    public int printAggregateScrape(AggregateMetrics state) {
        return state.print(state.splitTopicAndPartitionIndexLabel, false, null);
    }

    @Benchmark
    @OperationsPerInvocation(TOPICS_PER_SCRAPE)
    public int printHighCardinalityScrape(HighCardinalityMetrics state) {
        return state.print(false, false, null);
    }

    private static String topicName(int index) {
        return "persistent://" + NAMESPACE + "/orders-" + index / 16 + "-partition-" + index % 16;
    }

    private static TopicStats smallTopicStats() {
        TopicStats stats = new TopicStats();
        stats.rateIn = 1000.25;
        stats.rateOut = 998.75;
        stats.throughputIn = 1_024_256.5;
        stats.throughputOut = 1_022_720.5;
        stats.averageMsgSize = 1024.25;
        stats.msgInCounter = 1_000_000;
        stats.bytesInCounter = 1_024_000_000;
        stats.msgBacklog = 2048;
        populateStorageStats(stats);
        return stats;
    }

    private static void populateSmallTopic(TopicStats stats, boolean detailed, boolean replicated) {
        stats.producersCount = 1;
        stats.subscriptionsCount = 1;
        stats.consumersCount = 2;
        String name = "order-subscription";
        AggregatedSubscriptionStats sub = addSubscription(stats, name, 900_000);
        if (replicated) {
            AggregatedReplicationStats remote = new AggregatedReplicationStats();
            remote.msgRateOut = stats.rateIn;
            remote.msgThroughputOut = stats.throughputIn;
            remote.replicationBacklog = 256;
            remote.connectedCount = 1;
            stats.replicationStats.put("remote-cluster", remote);
        }
        if (detailed) {
            addProducer(stats, "order-producer", 10_000);
            sub.backlogAgeSeconds = stats.backlogAgeSeconds;
            sub.bucketDelayedIndexStats.put("loaded", new TopicMetricBean(
                    "pulsar_delayed_message_index_loaded", 512, new String[]{"bucket", "loaded"}));
            ConsumerFactory consumers = new ConsumerFactory();
            for (int c = 0; c < stats.consumersCount; c++) {
                Consumer consumer = consumers.create(name, SubType.Failover, topicName(0), 20_000 + c,
                        "order-consumer-" + c);
                addConsumer(sub, consumer, 256);
            }
        } else {
            stats.msgOutCounter = sub.msgOutCounter;
            stats.bytesOutCounter = sub.bytesOutCounter;
            sub.unackedMessages = 512;
        }
    }

    private static void populateStorageStats(TopicStats stats) {
        stats.backlogQuotaLimit = 1_073_741_824;
        stats.backlogQuotaLimitTime = 3600;
        stats.backlogAgeSeconds = 300;
        stats.managedLedgerStats.storageSize = 67_108_864;
        stats.managedLedgerStats.storageLogicalSize = 22_369_621;
        stats.managedLedgerStats.backlogSize = stats.msgBacklog * MESSAGE_BYTES;
        stats.managedLedgerStats.storageWriteRate = stats.rateIn;
        stats.managedLedgerStats.storageReadRate = stats.rateOut;
        for (int i = 0; i < 512; i++) {
            stats.managedLedgerStats.storageWriteLatencyBuckets.addValue(750);
            stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.addValue(500);
            stats.managedLedgerStats.entrySizeBuckets.addValue(1024);
        }
        stats.managedLedgerStats.storageWriteLatencyBuckets.refresh();
        stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.refresh();
        stats.managedLedgerStats.entrySizeBuckets.refresh();
    }

    private static void addProducer(TopicStats stats, String name, long id) {
        AggregatedProducerStats producer = new AggregatedProducerStats();
        producer.producerId = id;
        producer.msgRateIn = stats.rateIn / stats.producersCount;
        producer.msgThroughputIn = stats.throughputIn / stats.producersCount;
        producer.averageMsgSize = stats.averageMsgSize;
        stats.producerStats.put(name, producer);
    }

    private static AggregatedSubscriptionStats addSubscription(TopicStats stats, String name, long messagesOut) {
        AggregatedSubscriptionStats sub = new AggregatedSubscriptionStats();
        sub.consumersCount = stats.consumersCount / stats.subscriptionsCount;
        sub.msgBacklog = stats.msgBacklog / stats.subscriptionsCount;
        sub.msgBacklogNoDelayed = sub.msgBacklog;
        sub.msgRateOut = stats.rateOut / stats.subscriptionsCount;
        sub.messageAckRate = sub.msgRateOut;
        sub.msgThroughputOut = stats.throughputOut / stats.subscriptionsCount;
        sub.msgOutCounter = messagesOut;
        sub.bytesOutCounter = messagesOut * MESSAGE_BYTES;
        stats.subscriptionStats.put(name, sub);
        return sub;
    }

    private static void addConsumer(AggregatedSubscriptionStats sub, Consumer consumer, long unackedMessages) {
        AggregatedConsumerStats stats = new AggregatedConsumerStats();
        stats.msgRateOut = sub.msgRateOut / sub.consumersCount;
        stats.msgAckRate = sub.messageAckRate / sub.consumersCount;
        stats.msgThroughputOut = sub.msgThroughputOut / sub.consumersCount;
        stats.unackedMessages = unackedMessages;
        stats.availablePermits = 1024;
        stats.msgOutCounter = sub.msgOutCounter / sub.consumersCount;
        stats.bytesOutCounter = sub.bytesOutCounter / sub.consumersCount;
        sub.consumerStat.put(consumer, stats);
    }

    // Use real Consumer label accessors in the measured loop. Share constructor dependencies during setup.
    private static class ConsumerFactory {
        private final Map<String, Subscription> subscriptions = new HashMap<>();
        private final Topic topic = mock(Topic.class, withSettings().stubOnly());
        private final TransportCnx cnx = mock(TransportCnx.class, withSettings().stubOnly());

        ConsumerFactory() {
            PulsarService pulsar = mock(PulsarService.class, withSettings().stubOnly());
            when(pulsar.getConfiguration()).thenReturn(new ServiceConfiguration());
            BrokerService broker = mock(BrokerService.class, withSettings().stubOnly());
            when(broker.getPulsar()).thenReturn(pulsar);
            when(topic.getBrokerService()).thenReturn(broker);
        }

        Consumer create(String subscriptionName, SubType type, String topicName, long id, String name) {
            Subscription sub = subscriptions.computeIfAbsent(subscriptionName, key -> {
                Subscription subscription = mock(Subscription.class, withSettings().stubOnly());
                when(subscription.getTopic()).thenReturn(topic);
                when(subscription.getName()).thenReturn(key);
                return subscription;
            });
            return new Consumer(sub, type, topicName, id, 0, name, true, cnx, "benchmark", Map.of(),
                    false, null, null, 0);
        }
    }
}
