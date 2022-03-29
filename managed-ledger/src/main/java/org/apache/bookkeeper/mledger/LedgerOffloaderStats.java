/**
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
package org.apache.bookkeeper.mledger;

import com.google.common.annotations.VisibleForTesting;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.naming.TopicName;


/**
 * Management Bean for a {@link LedgerOffloader}.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Stable
public final class LedgerOffloaderStats implements Runnable {
    private static final String TOPIC_LABEL = "topic";
    private static final String NAMESPACE_LABEL = "namespace";
    private static final String UNKNOWN = "unknown";

    private final boolean exposeLedgerMetrics;
    private final boolean exposeTopicLevelMetrics;
    private final int interval;

    private Counter offloadError;
    private Gauge offloadRate;
    private Summary readLedgerLatency;
    private Counter writeStorageError;
    private Counter readOffloadError;
    private Gauge readOffloadRate;
    private Summary readOffloadIndexLatency;
    private Summary readOffloadDataLatency;

    private Map<String, String> topic2Namespace;
    private Map<String, Pair<LongAdder, LongAdder>> offloadAndReadOffloadBytesMap;

    private static volatile LedgerOffloaderStats instance;
    private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);

    private LedgerOffloaderStats(boolean exposeLedgerMetrics, boolean exposeTopicLevelMetrics, int interval) {
        this.interval = interval;
        this.exposeLedgerMetrics = exposeLedgerMetrics;
        this.exposeTopicLevelMetrics = exposeTopicLevelMetrics;
        if (!exposeLedgerMetrics) {
            return;
        }

        this.topic2Namespace = new ConcurrentHashMap<>();
        this.offloadAndReadOffloadBytesMap = new ConcurrentHashMap<>();

        String[] labels = exposeTopicLevelMetrics
                ? new String[]{NAMESPACE_LABEL, TOPIC_LABEL} : new String[]{NAMESPACE_LABEL};

        this.offloadError = Counter.build("brk_ledgeroffloader_offload_error", "-")
                .labelNames(labels).create().register();
        this.offloadRate = Gauge.build("brk_ledgeroffloader_offload_rate", "-")
                .labelNames(labels).create().register();

        this.readOffloadError = Counter.build("brk_ledgeroffloader_read_offload_error", "-")
                .labelNames(labels).create().register();
        this.readOffloadRate = Gauge.build("brk_ledgeroffloader_read_offload_rate", "-")
                .labelNames(labels).create().register();
        this.writeStorageError = Counter.build("brk_ledgeroffloader_write_storage_error", "-")
                .labelNames(labels).create().register();

        this.readOffloadIndexLatency = Summary.build("brk_ledgeroffloader_read_offload_index_latency", "-")
                .labelNames(labels).create().register();
        this.readOffloadDataLatency = Summary.build("brk_ledgeroffloader_read_offload_data_latency", "-")
                .labelNames(labels).create().register();
        this.readLedgerLatency = Summary.build("brk_ledgeroffloader_read_ledger_latency", "-")
                .labelNames(labels).create().register();
    }

    public void recordOffloadError(String topic) {
        if (!exposeLedgerMetrics) {
            return;
        }

        String[] labelValues = this.labelValues(topic);
        this.offloadError.labels(labelValues).inc();
    }

    public void recordOffloadBytes(String topic, long size) {
        if (!exposeLedgerMetrics) {
            return;
        }

        topic = StringUtils.isBlank(topic) ? UNKNOWN : topic;
        Pair<LongAdder, LongAdder> pair = this.offloadAndReadOffloadBytesMap
                .computeIfAbsent(topic, __ -> new ImmutablePair<>(new LongAdder(), new LongAdder()));
        pair.getLeft().add(size);
    }

    public void recordReadLedgerLatency(String topic, long latency, TimeUnit unit) {
        if (!exposeLedgerMetrics) {
            return;
        }

        String[] labelValues = this.labelValues(topic);
        this.readLedgerLatency.labels(labelValues).observe(unit.toMicros(latency));
    }

    public void recordWriteToStorageError(String topic) {
        if (!exposeLedgerMetrics) {
            return;
        }

        String[] labelValues = this.labelValues(topic);
        this.writeStorageError.labels(labelValues).inc();
    }

    public void recordReadOffloadError(String topic) {
        if (!exposeLedgerMetrics) {
            return;
        }

        String[] labelValues = this.labelValues(topic);
        this.readOffloadError.labels(labelValues).inc();
    }

    public void recordReadOffloadBytes(String topic, long size) {
        if (!exposeLedgerMetrics) {
            return;
        }

        topic = StringUtils.isBlank(topic) ? UNKNOWN : topic;
        Pair<LongAdder, LongAdder> pair = this.offloadAndReadOffloadBytesMap
                .computeIfAbsent(topic, __ -> new ImmutablePair<>(new LongAdder(), new LongAdder()));
        pair.getRight().add(size);
    }

    public void recordReadOffloadIndexLatency(String topic, long latency, TimeUnit unit) {
        if (!exposeLedgerMetrics) {
            return;
        }

        String[] labelValues = this.labelValues(topic);
        this.readOffloadIndexLatency.labels(labelValues).observe(unit.toMicros(latency));
    }

    public void recordReadOffloadDataLatency(String topic, long latency, TimeUnit unit) {
        if (!exposeLedgerMetrics) {
            return;
        }

        String[] labelValues = this.labelValues(topic);
        this.readOffloadDataLatency.labels(labelValues).observe(unit.toMicros(latency));
    }


    private String[] labelValues(String topic) {
        if (StringUtils.isBlank(topic)) {
            return this.exposeTopicLevelMetrics ? new String[]{UNKNOWN, UNKNOWN} : new String[]{UNKNOWN};
        }
        String namespace = this.topic2Namespace.computeIfAbsent(topic, t -> {
            try {
                return TopicName.get(t).getNamespace();
            } catch (Throwable th) {
                return UNKNOWN;
            }
        });
        return this.exposeTopicLevelMetrics ? new String[]{namespace, topic} : new String[]{namespace};
    }


    public static void initialize(boolean exposeLedgerMetrics,
                                  boolean exposeTopicLevelMetrics, ScheduledExecutorService executor, int interval) {
        if (INITIALIZED.compareAndSet(false, true)) {
            instance = new LedgerOffloaderStats(exposeLedgerMetrics, exposeTopicLevelMetrics, interval);
            if (exposeLedgerMetrics && null != executor) {
                executor.scheduleAtFixedRate(instance, interval, interval, TimeUnit.SECONDS);
            }
        }
    }

    public static LedgerOffloaderStats getInstance() {
        if (instance == null) {
            instance = new LedgerOffloaderStats(false, false, 0);
        }

        return instance;
    }

    @Override
    public void run() {
        this.offloadAndReadOffloadBytesMap.forEach((topic, pair) -> {
            String[] labelValues = this.labelValues(topic);

            double interval = this.interval;
            long offloadBytes = pair.getLeft().sumThenReset();
            long readOffloadBytes = pair.getRight().sumThenReset();

            this.offloadRate.labels(labelValues).set(offloadBytes / interval);
            this.readOffloadRate.labels(labelValues).set(readOffloadBytes / interval);
        });
    }

    @VisibleForTesting
    public long getOffloadBytes(String topic) {
        if (this.exposeTopicLevelMetrics) {
            Pair<LongAdder, LongAdder> pair = this.offloadAndReadOffloadBytesMap.get(topic);
            return pair.getLeft().sum();
        }

        String namespace = this.topic2Namespace.get(topic);
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
        return (long) this.offloadError.labels(labels).get();
    }

    @VisibleForTesting
    public long getWriteStorageError(String topic) {
        String[] labels = this.labelValues(topic);
        return (long) this.writeStorageError.labels(labels).get();
    }

    @VisibleForTesting
    public long getReadOffloadError(String topic) {
        String[] labels = this.labelValues(topic);
        return (long) this.readOffloadError.labels(labels).get();
    }

    @VisibleForTesting
    public long getReadOffloadBytes(String topic) {
        if (this.exposeTopicLevelMetrics) {
            Pair<LongAdder, LongAdder> pair = this.offloadAndReadOffloadBytesMap.get(topic);
            return pair.getRight().sum();
        }

        String namespace = this.topic2Namespace.get(topic);
        List<String> topics = this.offloadAndReadOffloadBytesMap.keySet().stream()
                .filter(topicName -> topicName.contains(namespace)).collect(Collectors.toList());

        long totalBytes = 0;
        for (String key : topics) {
            totalBytes += this.offloadAndReadOffloadBytesMap.get(key).getRight().sum();
        }
        return totalBytes;
    }

    @VisibleForTesting
    public Summary.Child.Value getReadLedgerLatency(String topic) {
        String[] labels = this.labelValues(topic);
        return this.readLedgerLatency.labels(labels).get();
    }

    @VisibleForTesting
    public Summary.Child.Value getReadOffloadIndexLatency(String topic) {
        String[] labels = this.labelValues(topic);
        return this.readOffloadIndexLatency.labels(labels).get();
    }

    @VisibleForTesting
    public Summary.Child.Value getReadOffloadDataLatency(String topic) {
        String[] labels = this.labelValues(topic);
        return this.readOffloadDataLatency.labels(labels).get();
    }
}
