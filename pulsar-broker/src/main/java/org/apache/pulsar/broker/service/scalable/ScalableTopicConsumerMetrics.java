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
package org.apache.pulsar.broker.service.scalable;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.BatchCallback;
import io.opentelemetry.api.metrics.Meter;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.CounterMetricFamily;
import io.prometheus.client.GaugeMetricFamily;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.Supplier;
import lombok.CustomLog;
import org.apache.pulsar.broker.service.scalable.SubscriptionCoordinator.ConsumerStats;
import org.apache.pulsar.broker.service.scalable.SubscriptionCoordinator.RebalanceReason;

/** Bounded-cardinality broker metrics, sharing the same state on both export paths. */
@CustomLog
final class ScalableTopicConsumerMetrics implements AutoCloseable {
    static final String CONSUMER_COUNT = "pulsar.broker.scalable_topic.consumer.count";
    static final String BLOCKED_COUNT = "pulsar.broker.scalable_topic.segment.blocked.count";
    static final String REBALANCE_COUNT = "pulsar.broker.scalable_topic.rebalance.count";
    private static final String PROM_CONSUMER_COUNT = "pulsar_scalable_topic_consumer_count";
    private static final String PROM_BLOCKED_COUNT = "pulsar_scalable_topic_segment_blocked_count";
    private static final String PROM_REBALANCE = "pulsar_scalable_topic_rebalance";
    private static final String CONSUMER_DESCRIPTION = "Consumer sessions coordinated by this broker.";
    private static final String BLOCKED_DESCRIPTION = "Segments waiting for ancestors to drain, per subscription.";
    private static final String REBALANCE_DESCRIPTION = "Consumer assignment recomputations.";
    private static final AttributeKey<String> CONSUMER_STATE =
            AttributeKey.stringKey("pulsar.scalable_topic.consumer.state");
    private static final AttributeKey<String> REBALANCE_REASON =
            AttributeKey.stringKey("pulsar.scalable_topic.rebalance.reason");
    private static final RebalanceReason[] REASONS = RebalanceReason.values();
    private static final List<String> STATES = List.of("active", "idle", "disconnected");
    private static final List<Attributes> STATE_ATTRIBUTES = STATES.stream()
            .map(state -> Attributes.of(CONSUMER_STATE, state)).toList();
    private static final List<String> REASON_NAMES = List.of(REASONS).stream()
            .map(reason -> reason.name().toLowerCase(Locale.ROOT)).toList();
    private static final List<Attributes> REASON_ATTRIBUTES = REASON_NAMES.stream()
            .map(reason -> Attributes.of(REBALANCE_REASON, reason)).toList();

    // CollectorRegistry is JVM-global in production. Share a registration when tests or embedded
    // deployments run several brokers in one JVM, aggregating their state per cluster on that path.
    private static final Map<CollectorRegistry, PrometheusCollector> COLLECTORS = new HashMap<>();

    private final Supplier<ConsumerStats> statsSupplier;
    private final AtomicLongArray rebalances = new AtomicLongArray(REASONS.length);
    private final String cluster;
    private final CollectorRegistry registry;
    private final BatchCallback callback;
    private boolean closed;

    ScalableTopicConsumerMetrics(Meter meter, CollectorRegistry registry, String cluster,
                                Supplier<ConsumerStats> statsSupplier) {
        this.registry = registry;
        this.cluster = cluster;
        this.statsSupplier = statsSupplier;
        var consumers = meter.upDownCounterBuilder(CONSUMER_COUNT)
                .setUnit("{consumer}").setDescription(CONSUMER_DESCRIPTION).buildObserver();
        var blocked = meter.upDownCounterBuilder(BLOCKED_COUNT)
                .setUnit("{segment}").setDescription(BLOCKED_DESCRIPTION).buildObserver();
        var rebalance = meter.counterBuilder(REBALANCE_COUNT)
                .setUnit("{rebalance}").setDescription(REBALANCE_DESCRIPTION).buildObserver();
        callback = meter.batchCallback(() -> {
            Snapshot snapshot = snapshot();
            long[] counts = snapshot.consumerCounts();
            for (int i = 0; i < counts.length; i++) {
                consumers.record(counts[i], STATE_ATTRIBUTES.get(i));
            }
            blocked.record(snapshot.stats.blocked());
            for (int i = 0; i < REASONS.length; i++) {
                rebalance.record(snapshot.rebalances[i], REASON_ATTRIBUTES.get(i));
            }
        }, consumers, blocked, rebalance);
        try {
            synchronized (COLLECTORS) {
                COLLECTORS.computeIfAbsent(registry, key -> {
                    PrometheusCollector collector = new PrometheusCollector();
                    key.register(collector);
                    return collector;
                }).sources.add(this);
            }
        } catch (RuntimeException e) {
            // A failed registration must not leave a callback in the broker's shared meter.
            callback.close();
            throw e;
        }
    }

    void recordRebalance(RebalanceReason reason) {
        rebalances.incrementAndGet(reason.ordinal());
    }

    private Snapshot snapshot() {
        ConsumerStats stats = statsSupplier.get();
        long[] counts = new long[REASONS.length];
        for (int i = 0; i < counts.length; i++) {
            counts[i] = rebalances.get(i);
        }
        return new Snapshot(stats, counts);
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        callback.close();
        synchronized (COLLECTORS) {
            PrometheusCollector collector = COLLECTORS.get(registry);
            collector.sources.remove(this);
            if (collector.sources.isEmpty()) {
                registry.unregister(collector);
                COLLECTORS.remove(registry);
            }
        }
    }

    private record Snapshot(ConsumerStats stats, long[] rebalances) {
        long[] consumerCounts() {
            return new long[]{stats.active(), stats.idle(), stats.disconnected()};
        }

        Snapshot plus(Snapshot other) {
            long[] combined = rebalances.clone();
            for (int i = 0; i < combined.length; i++) {
                combined[i] += other.rebalances[i];
            }
            return new Snapshot(stats.plus(other.stats), combined);
        }
    }

    private static final class PrometheusCollector extends Collector implements Collector.Describable {
        private final List<ScalableTopicConsumerMetrics> sources = new CopyOnWriteArrayList<>();

        @Override
        public List<MetricFamilySamples> describe() {
            return List.of(new GaugeMetricFamily(PROM_CONSUMER_COUNT, CONSUMER_DESCRIPTION,
                            List.of("cluster", "consumer_state")),
                    new GaugeMetricFamily(PROM_BLOCKED_COUNT, BLOCKED_DESCRIPTION, List.of("cluster")),
                    new CounterMetricFamily(PROM_REBALANCE, REBALANCE_DESCRIPTION,
                            List.of("cluster", "rebalance_reason")));
        }

        @Override
        public List<MetricFamilySamples> collect() {
            var consumers = new GaugeMetricFamily(PROM_CONSUMER_COUNT, CONSUMER_DESCRIPTION,
                    List.of("cluster", "consumer_state"));
            var blocked = new GaugeMetricFamily(PROM_BLOCKED_COUNT, BLOCKED_DESCRIPTION, List.of("cluster"));
            var rebalance = new CounterMetricFamily(PROM_REBALANCE, REBALANCE_DESCRIPTION,
                    List.of("cluster", "rebalance_reason"));
            Map<String, Snapshot> clusters = new HashMap<>();
            for (ScalableTopicConsumerMetrics source : sources) {
                try {
                    clusters.merge(source.cluster, source.snapshot(), Snapshot::plus);
                } catch (RuntimeException e) {
                    // This registry also exports existing broker metrics. A failed new source
                    // must not abort their scrape, or prevent other brokers from being collected.
                    log.debug().attr("cluster", source.cluster).exceptionMessage(e)
                            .log("Skipping failed scalable-topic consumer metrics snapshot");
                }
            }
            clusters.forEach((cluster, snapshot) -> {
                long[] counts = snapshot.consumerCounts();
                for (int i = 0; i < counts.length; i++) {
                    consumers.addMetric(List.of(cluster, STATES.get(i)), counts[i]);
                }
                blocked.addMetric(List.of(cluster), snapshot.stats.blocked());
                for (int i = 0; i < REASONS.length; i++) {
                    rebalance.addMetric(List.of(cluster, REASON_NAMES.get(i)), snapshot.rebalances[i]);
                }
            });
            return new ArrayList<>(List.of(consumers, blocked, rebalance));
        }
    }
}
