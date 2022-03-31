package org.apache.bookkeeper.mledger.impl;

import com.google.common.annotations.VisibleForTesting;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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

    private final boolean exposeTopicLevelMetrics;
    private final int interval;

    private final Counter offloadError;
    private final Gauge offloadRate;
    private final Summary readLedgerLatency;
    private final Counter writeStorageError;
    private final Counter readOffloadError;
    private final Gauge readOffloadRate;
    private final Summary readOffloadIndexLatency;
    private final Summary readOffloadDataLatency;

    private final Map<String, String> topic2Namespace;
    private final Map<String, Pair<LongAdder, LongAdder>> offloadAndReadOffloadBytesMap;

    public LedgerOffloaderStatsImpl(boolean exposeTopicLevelMetrics,
                                    ScheduledExecutorService scheduler, int interval) {
        this.interval = interval;
        this.exposeTopicLevelMetrics = exposeTopicLevelMetrics;
        if (null != scheduler) {
            scheduler.scheduleAtFixedRate(this, interval, interval, TimeUnit.SECONDS);
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
        String[] labelValues = this.labelValues(topic);
        this.offloadError.labels(labelValues).inc();
    }

    public void recordOffloadBytes(String topic, long size) {
        topic = StringUtils.isBlank(topic) ? UNKNOWN : topic;
        Pair<LongAdder, LongAdder> pair = this.offloadAndReadOffloadBytesMap
                .computeIfAbsent(topic, __ -> new ImmutablePair<>(new LongAdder(), new LongAdder()));
        pair.getLeft().add(size);
    }

    public void recordReadLedgerLatency(String topic, long latency, TimeUnit unit) {
        String[] labelValues = this.labelValues(topic);
        this.readLedgerLatency.labels(labelValues).observe(unit.toMicros(latency));
    }

    public void recordWriteToStorageError(String topic) {
        String[] labelValues = this.labelValues(topic);
        this.writeStorageError.labels(labelValues).inc();
    }

    public void recordReadOffloadError(String topic) {
        String[] labelValues = this.labelValues(topic);
        this.readOffloadError.labels(labelValues).inc();
    }

    public void recordReadOffloadBytes(String topic, long size) {
        topic = StringUtils.isBlank(topic) ? UNKNOWN : topic;
        Pair<LongAdder, LongAdder> pair = this.offloadAndReadOffloadBytesMap
                .computeIfAbsent(topic, __ -> new ImmutablePair<>(new LongAdder(), new LongAdder()));
        pair.getRight().add(size);
    }

    public void recordReadOffloadIndexLatency(String topic, long latency, TimeUnit unit) {
        String[] labelValues = this.labelValues(topic);
        this.readOffloadIndexLatency.labels(labelValues).observe(unit.toMicros(latency));
    }

    public void recordReadOffloadDataLatency(String topic, long latency, TimeUnit unit) {
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

    @Override
    public void close() throws Exception {
        CollectorRegistry.defaultRegistry.unregister(this.offloadError);
        CollectorRegistry.defaultRegistry.unregister(this.offloadRate);
        CollectorRegistry.defaultRegistry.unregister(this.readLedgerLatency);
        CollectorRegistry.defaultRegistry.unregister(this.writeStorageError);
        CollectorRegistry.defaultRegistry.unregister(this.readOffloadError);
        CollectorRegistry.defaultRegistry.unregister(this.readOffloadRate);
        CollectorRegistry.defaultRegistry.unregister(this.readOffloadIndexLatency);
        CollectorRegistry.defaultRegistry.unregister(this.readOffloadDataLatency);
        this.topic2Namespace.clear();
        this.offloadAndReadOffloadBytesMap.clear();
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
