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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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
 * to compare bytes allocated per topic. Snapshot construction and output buffer allocation are outside measurement;
 * metric stream creation, metric grouping, flushing and release are included.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class TopicStatsPrintBenchmark {
    private static final int TOPICS_PER_SCRAPE = 64;
    private static final String CLUSTER = "benchmark-cluster";
    private static final String NAMESPACE = "benchmark-tenant/benchmark-namespace";

    @Param({"false", "true"})
    public boolean splitTopicAndPartitionIndexLabel;

    @Param({"false", "true"})
    public boolean includeEntities;

    private final String[] topics = new String[TOPICS_PER_SCRAPE];
    private final String[] customLabels = {"environment", "production", "region", "us-east"};
    private TopicStats stats;
    private ByteBuf outputBuffer;
    private SimpleTextOutputStream output;

    @Setup
    public void setup() {
        for (int i = 0; i < topics.length; i++) {
            topics[i] = "persistent://" + NAMESPACE + "/orders-" + i / 16 + "-partition-" + i % 16;
        }
        stats = new TopicStats();
        stats.rateIn = 1000.25;
        stats.rateOut = 998.75;
        stats.throughputIn = 1_024_256.5;
        stats.throughputOut = 1_022_720.5;
        stats.averageMsgSize = 1024.25;
        stats.msgInCounter = 1_000_000;
        stats.bytesInCounter = 1_024_000_000;
        stats.msgBacklog = 2048;
        stats.backlogQuotaLimit = 1_073_741_824;
        stats.backlogQuotaLimitTime = 3600;
        stats.backlogAgeSeconds = 300;
        stats.managedLedgerStats.storageSize = 67_108_864;
        stats.managedLedgerStats.storageLogicalSize = 22_369_621;
        stats.managedLedgerStats.backlogSize = 2_097_152;
        stats.managedLedgerStats.storageWriteRate = 1000.25;
        stats.managedLedgerStats.storageReadRate = 998.75;
        for (int i = 0; i < 512; i++) {
            stats.managedLedgerStats.storageWriteLatencyBuckets.addValue(750);
            stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.addValue(500);
            stats.managedLedgerStats.entrySizeBuckets.addValue(1024);
        }
        stats.managedLedgerStats.storageWriteLatencyBuckets.refresh();
        stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.refresh();
        stats.managedLedgerStats.entrySizeBuckets.refresh();
        if (includeEntities) {
            addEntityStats();
        }
        outputBuffer = Unpooled.buffer(4 * 1024 * 1024);
        output = new SimpleTextOutputStream(outputBuffer);
    }

    private void addEntityStats() {
        stats.producersCount = 1;
        stats.subscriptionsCount = 1;
        stats.consumersCount = 2;
        AggregatedProducerStats producerStats = new AggregatedProducerStats();
        producerStats.producerId = 10_000;
        producerStats.msgRateIn = stats.rateIn;
        producerStats.msgThroughputIn = stats.throughputIn;
        producerStats.averageMsgSize = stats.averageMsgSize;
        stats.producerStats.put("order-producer", producerStats);

        AggregatedSubscriptionStats subscriptionStats = new AggregatedSubscriptionStats();
        subscriptionStats.msgBacklog = stats.msgBacklog;
        subscriptionStats.msgBacklogNoDelayed = stats.msgBacklog;
        subscriptionStats.backlogAgeSeconds = stats.backlogAgeSeconds;
        subscriptionStats.msgRateOut = stats.rateOut;
        subscriptionStats.msgThroughputOut = stats.throughputOut;
        subscriptionStats.messageAckRate = stats.rateOut;
        subscriptionStats.msgOutCounter = 900_000;
        subscriptionStats.bytesOutCounter = 921_600_000;
        subscriptionStats.consumersCount = 2;
        subscriptionStats.bucketDelayedIndexStats.put("loaded", new TopicMetricBean(
                "pulsar_delayed_message_index_loaded", 512, new String[]{"bucket", "loaded"}));
        stats.subscriptionStats.put("order-subscription", subscriptionStats);

        // Real consumers keep Mockito invocation accounting out of the measured consumer label accessors.
        PulsarService pulsar = mock(PulsarService.class);
        when(pulsar.getConfiguration()).thenReturn(new ServiceConfiguration());
        BrokerService broker = mock(BrokerService.class);
        when(broker.getPulsar()).thenReturn(pulsar);
        Topic topic = mock(Topic.class);
        when(topic.getBrokerService()).thenReturn(broker);
        Subscription subscription = mock(Subscription.class);
        when(subscription.getTopic()).thenReturn(topic);
        when(subscription.getName()).thenReturn("order-subscription");
        TransportCnx cnx = mock(TransportCnx.class);
        for (int i = 0; i < stats.consumersCount; i++) {
            Consumer consumer = new Consumer(subscription, SubType.Failover, topics[0], 20_000 + i,
                    0, "order-consumer-" + i, true, cnx, "benchmark", Map.of(), false, null, null, 0);
            AggregatedConsumerStats consumerStats = new AggregatedConsumerStats();
            consumerStats.msgRateOut = 499.375;
            consumerStats.msgAckRate = 499.375;
            consumerStats.msgThroughputOut = 511_360.25;
            consumerStats.unackedMessages = 256;
            consumerStats.availablePermits = 1024;
            consumerStats.msgOutCounter = 450_000;
            consumerStats.bytesOutCounter = 460_800_000;
            subscriptionStats.consumerStat.put(consumer, consumerStats);
        }
        AggregatedReplicationStats replicationStats = new AggregatedReplicationStats();
        replicationStats.msgRateOut = stats.rateIn;
        replicationStats.msgThroughputOut = stats.throughputIn;
        replicationStats.replicationBacklog = 256;
        replicationStats.connectedCount = 1;
        stats.replicationStats.put("remote-cluster", replicationStats);
    }

    @Benchmark
    @OperationsPerInvocation(TOPICS_PER_SCRAPE)
    public int printScrape() {
        outputBuffer.clear();
        PrometheusMetricStreams streams = new PrometheusMetricStreams();
        try {
            for (String topic : topics) {
                TopicStats.printTopicStats(streams, stats, Optional.empty(), CLUSTER, NAMESPACE, topic,
                        splitTopicAndPartitionIndexLabel, true, customLabels);
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
