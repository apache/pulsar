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

import com.google.common.annotations.VisibleForTesting;
import io.prometheus.metrics.core.datapoints.DistributionDataPoint;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.core.metrics.Summary;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import org.apache.bookkeeper.mledger.LedgerOffloaderStats;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.naming.TopicName;

public final class LedgerOffloaderStatsImpl implements LedgerOffloaderStats, Runnable {
    private static final String TOPIC_LABEL = "topic";
    private static final String NAMESPACE_LABEL = "namespace";
    private static final String UNKNOWN = "unknown";
    private static final String STATUS = "status";
    private static final String SUCCEED = "succeed";
    private static final String FAILED = "failed";
    /** simpleclient's default sliding window for summary quantiles; the 1.x default is 5 minutes. */
    private static final long QUANTILE_WINDOW_SECONDS = TimeUnit.MINUTES.toSeconds(10);

    private final boolean exposeTopicLevelMetrics;
    private final int interval;

    private final Counter offloadError;
    private final Gauge offloadRate;
    private final Counter deleteOffloadOps;
    private final Summary readLedgerLatency;
    private final Counter writeStorageError;
    private final Counter readOffloadError;
    private final Counter readOffloadBytes;
    private final Gauge readOffloadRate;
    private final Summary readOffloadIndexLatency;
    private final Summary readOffloadDataLatency;

    private final Map<String, Long> topicAccess;
    private final Map<String, Pair<LongAdder, LongAdder>> offloadAndReadOffloadBytesMap;

    final AtomicBoolean closed = new AtomicBoolean(false);

     private LedgerOffloaderStatsImpl(boolean exposeTopicLevelMetrics,
                                     ScheduledExecutorService scheduler, int interval) {
        this.interval = interval;
        this.exposeTopicLevelMetrics = exposeTopicLevelMetrics;
        if (null != scheduler) {
            scheduler.scheduleAtFixedRate(this, interval, interval, TimeUnit.SECONDS);
        }

        this.topicAccess = new ConcurrentHashMap<>();
        this.offloadAndReadOffloadBytesMap = new ConcurrentHashMap<>();

        String[] labels = exposeTopicLevelMetrics
                ? new String[]{NAMESPACE_LABEL, TOPIC_LABEL} : new String[]{NAMESPACE_LABEL};

        this.offloadError = Counter.builder().name("brk_ledgeroffloader_offload_error").help("-")
                .labelNames(labels).register();
        this.offloadRate = Gauge.builder().name("brk_ledgeroffloader_offload_rate").help("-")
                .labelNames(labels).register();

        this.readOffloadError = Counter.builder().name("brk_ledgeroffloader_read_offload_error").help("-")
                .labelNames(labels).register();
        this.readOffloadRate = Gauge.builder().name("brk_ledgeroffloader_read_offload_rate").help("-")
                .labelNames(labels).register();
        this.readOffloadBytes = Counter.builder().name("brk_ledgeroffloader_read_bytes").help("-")
                .labelNames(labels).register();
        this.writeStorageError = Counter.builder().name("brk_ledgeroffloader_write_storage_error").help("-")
                .labelNames(labels).register();

        this.readOffloadIndexLatency = latencySummary("brk_ledgeroffloader_read_offload_index_latency", labels);
        this.readOffloadDataLatency = latencySummary("brk_ledgeroffloader_read_offload_data_latency", labels);
        this.readLedgerLatency = latencySummary("brk_ledgeroffloader_read_ledger_latency", labels);

        String[] deleteOpsLabels = exposeTopicLevelMetrics
                ? new String[]{NAMESPACE_LABEL, TOPIC_LABEL, STATUS} : new String[]{NAMESPACE_LABEL, STATUS};
        this.deleteOffloadOps = Counter.builder().name("brk_ledgeroffloader_delete_offload_ops").help("-")
                .labelNames(deleteOpsLabels).register();
    }

    private static Summary latencySummary(String name, String[] labels) {
        return Summary.builder().name(name).help("-")
                .labelNames(labels)
                .quantile(0.50, 0.01)
                .quantile(0.95, 0.01)
                .quantile(0.99, 0.01)
                .quantile(1, 0.01)
                .maxAgeSeconds(QUANTILE_WINDOW_SECONDS)
                .register();
    }


    private static LedgerOffloaderStats instance;
    public static synchronized LedgerOffloaderStats getInstance(boolean exposeTopicLevelMetrics,
                                                                ScheduledExecutorService scheduler, int interval) {
        if (null == instance) {
            instance = new LedgerOffloaderStatsImpl(exposeTopicLevelMetrics, scheduler, interval);
        }

        return instance;
    }

    @Override
    public void recordOffloadError(String topic) {
        String[] labelValues = this.labelValues(topic);
        this.offloadError.labelValues(labelValues).inc();
        this.addOrUpdateTopicAccess(topic);
    }

    @Override
    public void recordOffloadBytes(String topic, long size) {
        topic = StringUtils.isBlank(topic) ? UNKNOWN : topic;
        Pair<LongAdder, LongAdder> pair = this.offloadAndReadOffloadBytesMap
                .computeIfAbsent(topic, __ -> new ImmutablePair<>(new LongAdder(), new LongAdder()));
        pair.getLeft().add(size);
        this.addOrUpdateTopicAccess(topic);
    }

    @Override
    public void recordReadLedgerLatency(String topic, long latency, TimeUnit unit) {
        String[] labelValues = this.labelValues(topic);
        this.readLedgerLatency.labelValues(labelValues).observe(unit.toMicros(latency));
        this.addOrUpdateTopicAccess(topic);
    }

    @Override
    public void recordWriteToStorageError(String topic) {
        String[] labelValues = this.labelValues(topic);
        this.writeStorageError.labelValues(labelValues).inc();
        this.addOrUpdateTopicAccess(topic);
    }

    @Override
    public void recordReadOffloadError(String topic) {
        String[] labelValues = this.labelValues(topic);
        this.readOffloadError.labelValues(labelValues).inc();
        this.addOrUpdateTopicAccess(topic);
    }

    @Override
    public void recordReadOffloadBytes(String topic, long size) {
        topic = StringUtils.isBlank(topic) ? UNKNOWN : topic;
        Pair<LongAdder, LongAdder> pair = this.offloadAndReadOffloadBytesMap
                .computeIfAbsent(topic, __ -> new ImmutablePair<>(new LongAdder(), new LongAdder()));
        pair.getRight().add(size);
        String[] labelValues = this.labelValues(topic);
        this.readOffloadBytes.labelValues(labelValues).inc(size);
        this.addOrUpdateTopicAccess(topic);
    }

    @Override
    public void recordReadOffloadIndexLatency(String topic, long latency, TimeUnit unit) {
        String[] labelValues = this.labelValues(topic);
        this.readOffloadIndexLatency.labelValues(labelValues).observe(unit.toMicros(latency));
        this.addOrUpdateTopicAccess(topic);
    }

    @Override
    public void recordReadOffloadDataLatency(String topic, long latency, TimeUnit unit) {
        String[] labelValues = this.labelValues(topic);
        this.readOffloadDataLatency.labelValues(labelValues).observe(unit.toMicros(latency));
        this.addOrUpdateTopicAccess(topic);
    }

    @Override
    public void recordDeleteOffloadOps(String topic, boolean succeed) {
        String status = succeed ? SUCCEED : FAILED;
        String[] labelValues = this.labelValues(topic, status);
        this.deleteOffloadOps.labelValues(labelValues).inc();
        this.addOrUpdateTopicAccess(topic);
    }

    private void addOrUpdateTopicAccess(String topic) {
        topic = StringUtils.isBlank(topic) ? UNKNOWN : topic;
        this.topicAccess.put(topic, System.currentTimeMillis());
    }

    private String[] labelValues(String topic, String status) {
        if (StringUtils.isBlank(topic)) {
            return exposeTopicLevelMetrics ? new String[]{UNKNOWN, UNKNOWN, status} : new String[]{UNKNOWN, status};
        }
        String namespace = this.getNamespace(topic);
        return this.exposeTopicLevelMetrics ? new String[]{namespace, topic, status} : new String[]{namespace, status};
    }

    private String[] labelValues(String topic) {
        if (StringUtils.isBlank(topic)) {
            return this.exposeTopicLevelMetrics ? new String[]{UNKNOWN, UNKNOWN} : new String[]{UNKNOWN};
        }
        String namespace = this.getNamespace(topic);
        return this.exposeTopicLevelMetrics ? new String[]{namespace, topic} : new String[]{namespace};
    }

    private String getNamespace(String topic) {
        try {
            return TopicName.get(topic).getNamespace();
        } catch (IllegalArgumentException ex) {
            return UNKNOWN;
        }
    }

    private void cleanExpiredTopicMetrics() {
        long now = System.currentTimeMillis();
        long timeout = TimeUnit.MINUTES.toMillis(2);

        topicAccess.entrySet().removeIf(entry -> {
            String topic = entry.getKey();
            long access = entry.getValue();

            if (now - access >= timeout) {
                this.offloadAndReadOffloadBytesMap.remove(topic);
                String[] labelValues = this.labelValues(topic);
                this.offloadError.remove(labelValues);
                this.offloadRate.remove(labelValues);
                this.readLedgerLatency.remove(labelValues);
                this.writeStorageError.remove(labelValues);
                this.readOffloadError.remove(labelValues);
                this.readOffloadRate.remove(labelValues);
                this.readOffloadIndexLatency.remove(labelValues);
                this.readOffloadDataLatency.remove(labelValues);

                labelValues = this.labelValues(topic, SUCCEED);
                this.deleteOffloadOps.remove(labelValues);
                labelValues = this.labelValues(topic, FAILED);
                this.deleteOffloadOps.remove(labelValues);

                return true;
            }
            return false;
        });
    }

    @Override
    public void run() {
        this.cleanExpiredTopicMetrics();

        this.offloadAndReadOffloadBytesMap.forEach((topic, pair) -> {
            String[] labelValues = this.labelValues(topic);

            double interval = this.interval;
            long offloadBytes = pair.getLeft().sumThenReset();
            long readOffloadBytes = pair.getRight().sumThenReset();

            this.offloadRate.labelValues(labelValues).set(offloadBytes / interval);
            this.readOffloadRate.labelValues(labelValues).set(readOffloadBytes / interval);
        });
    }

    @Override
    public synchronized void close() throws Exception {
        if (instance == this && this.closed.compareAndSet(false, true)) {
            PrometheusRegistry.defaultRegistry.unregister(this.offloadError);
            PrometheusRegistry.defaultRegistry.unregister(this.offloadRate);
            PrometheusRegistry.defaultRegistry.unregister(this.readLedgerLatency);
            PrometheusRegistry.defaultRegistry.unregister(this.writeStorageError);
            PrometheusRegistry.defaultRegistry.unregister(this.readOffloadError);
            PrometheusRegistry.defaultRegistry.unregister(this.readOffloadBytes);
            PrometheusRegistry.defaultRegistry.unregister(this.readOffloadRate);
            PrometheusRegistry.defaultRegistry.unregister(this.readOffloadIndexLatency);
            PrometheusRegistry.defaultRegistry.unregister(this.readOffloadDataLatency);
            PrometheusRegistry.defaultRegistry.unregister(this.deleteOffloadOps);
            instance = null;
        }
    }

    @VisibleForTesting
    public long getOffloadBytes(String topic) {
        if (this.exposeTopicLevelMetrics) {
            Pair<LongAdder, LongAdder> pair = this.offloadAndReadOffloadBytesMap.get(topic);
            return pair.getLeft().sum();
        }

        String namespace = this.getNamespace(topic);
        List<String> topics = this.offloadAndReadOffloadBytesMap.keySet().stream()
                .filter(topicName -> topicName.contains(namespace)).collect(Collectors.toList());

        long totalBytes = 0;
        for (String key : topics) {
            totalBytes += this.offloadAndReadOffloadBytesMap.get(key).getLeft().sum();
        }
        return totalBytes;
    }

    @VisibleForTesting
    public long getOffloadError(String topic) {
        String[] labels = this.labelValues(topic);
        return this.offloadError.labelValues(labels).getLongValue();
    }

    @VisibleForTesting
    public long getWriteStorageError(String topic) {
        String[] labels = this.labelValues(topic);
        return this.writeStorageError.labelValues(labels).getLongValue();
    }

    @VisibleForTesting
    public long getReadOffloadError(String topic) {
        String[] labels = this.labelValues(topic);
        return this.readOffloadError.labelValues(labels).getLongValue();
    }

    @VisibleForTesting
    public long getReadOffloadBytes(String topic) {
        if (this.exposeTopicLevelMetrics) {
            Pair<LongAdder, LongAdder> pair = this.offloadAndReadOffloadBytesMap.get(topic);
            return pair.getRight().sum();
        }

        String namespace = this.getNamespace(topic);
        List<String> topics = this.offloadAndReadOffloadBytesMap.keySet().stream()
                .filter(topicName -> topicName.contains(namespace)).collect(Collectors.toList());

        long totalBytes = 0;
        for (String key : topics) {
            totalBytes += this.offloadAndReadOffloadBytesMap.get(key).getRight().sum();
        }
        return totalBytes;
    }

    @VisibleForTesting
    public DistributionDataPoint getReadLedgerLatency(String topic) {
        String[] labels = this.labelValues(topic);
        return this.readLedgerLatency.labelValues(labels);
    }

    @VisibleForTesting
    public DistributionDataPoint getReadOffloadIndexLatency(String topic) {
        String[] labels = this.labelValues(topic);
        return this.readOffloadIndexLatency.labelValues(labels);
    }

    @VisibleForTesting
    public DistributionDataPoint getReadOffloadDataLatency(String topic) {
        String[] labels = this.labelValues(topic);
        return this.readOffloadDataLatency.labelValues(labels);
    }
}
