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
package org.apache.bookkeeper.mledger.impl;


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import lombok.Getter;
import org.apache.bookkeeper.mledger.LedgerOffloaderMXBean;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.stats.Rate;

public class LedgerOffloaderMXBeanImpl implements LedgerOffloaderMXBean {

    private static final int DEFAULT_SIZE = 4;
    public static final long[] READ_ENTRY_LATENCY_BUCKETS_USEC = {500, 1_000, 5_000, 10_000, 20_000, 50_000, 100_000,
            200_000, 1000_000};

    private final String driverName;

    private final Map<String, OffloadTopicMetrics> metricsMap = new ConcurrentHashMap<>(DEFAULT_SIZE);

    public LedgerOffloaderMXBeanImpl(String driverName) {
        this.driverName = driverName;
    }

    @Override
    public String getDriverName() {
        return this.driverName;
    }

    @Override
    public long getOffloadErrors(String topic) {
        LongAdder errors = this.getMetricsByTopic(topic).offloadErrorCount;
        return null == errors ? 0L : errors.sum();
    }

    @Override
    public Rate getOffloadRate(String topic) {
        Rate rate = this.getMetricsByTopic(topic).offloadRate;
        rate.calculateRate();
        return rate;
    }

    @Override
    public StatsBuckets getReadLedgerLatencyBuckets(String topic) {
        StatsBuckets buckets = this.getMetricsByTopic(topic).readLedgerLatencyBuckets;
        if (null != buckets) {
            buckets.refresh();
        }
        return buckets;
    }

    @Override
    public long getWriteToStorageErrors(String topic) {
        LongAdder errors = this.getMetricsByTopic(topic).writeStorageErrorCount;
        return null == errors ? 0L : errors.sum();
    }


    @Override
    public StatsBuckets getReadOffloadIndexLatencyBuckets(String topic) {
        StatsBuckets buckets = this.getMetricsByTopic(topic).readOffloadIndexLatencyBuckets;
        if (null != buckets) {
            buckets.refresh();
        }
        return buckets;
    }

    @Override
    public StatsBuckets getReadOffloadDataLatencyBuckets(String topic) {
        StatsBuckets buckets = this.getMetricsByTopic(topic).readOffloadDataLatencyBuckets;
        if (null != buckets) {
            buckets.refresh();
        }
        return buckets;
    }

    @Override
    public Rate getReadOffloadRate(String topic) {
        Rate rate = this.getMetricsByTopic(topic).readOffloadDataRate;
        rate.calculateRate();
        return rate;
    }

    @Override
    public long getReadOffloadErrors(String topic) {
        LongAdder errors = this.getMetricsByTopic(topic).readOffloadErrorCount;
        return null == errors ? 0L : errors.sum();
    }

    public void recordOffloadError(String topicName) {
        if (StringUtils.isBlank(topicName)) {
            return;
        }
        LongAdder adder = this.getMetricsByTopic(topicName).offloadErrorCount;
        adder.add(1L);
    }

    public void recordOffloadBytes(String topicName, long size) {
        if (StringUtils.isBlank(topicName)) {
            return;
        }
        Rate rate = this.getMetricsByTopic(topicName).offloadRate;
        rate.recordEvent(size);
    }

    public void recordReadLedgerLatency(String topicName, long latency, TimeUnit unit) {
        if (StringUtils.isBlank(topicName)) {
            return;
        }
        StatsBuckets statsBuckets = this.getMetricsByTopic(topicName).readLedgerLatencyBuckets;
        statsBuckets.addValue(unit.toMicros(latency));
    }

    public void recordWriteToStorageError(String topicName) {
        if (StringUtils.isBlank(topicName)) {
            return;
        }
        LongAdder adder = this.getMetricsByTopic(topicName).writeStorageErrorCount;
        adder.add(1L);
    }

    public void recordReadOffloadError(String topicName) {
        if (StringUtils.isBlank(topicName)) {
            return;
        }
        LongAdder adder = this.getMetricsByTopic(topicName).readOffloadErrorCount;
        adder.add(1L);
    }

    public void recordReadOffloadBytes(String topicName, long size) {
        if (StringUtils.isBlank(topicName)) {
            return;
        }
        Rate rate = this.getMetricsByTopic(topicName).readOffloadDataRate;
        rate.recordEvent(size);
    }

    public void recordReadOffloadIndexLatency(String topicName, long latency, TimeUnit unit) {
        if (StringUtils.isBlank(topicName)) {
            return;
        }
        StatsBuckets statsBuckets = this.getMetricsByTopic(topicName).readOffloadIndexLatencyBuckets;
        statsBuckets.addValue(unit.toMicros(latency));
    }

    public void recordReadOffloadDataLatency(String topicName, long latency, TimeUnit unit) {
        if (StringUtils.isBlank(topicName)) {
            return;
        }
        StatsBuckets statsBuckets = this.getMetricsByTopic(topicName).readOffloadDataLatencyBuckets;
        statsBuckets.addValue(unit.toMicros(latency));
    }


    private OffloadTopicMetrics getMetricsByTopic(String topicName) {
        return this.metricsMap.computeIfAbsent(topicName, k -> new OffloadTopicMetrics());
    }


    public OffloadTopicMetrics removeMetricsByTopic(String topicName) {
        OffloadTopicMetrics metrics = this.metricsMap.remove(topicName);
        if (null != metrics) {
            metrics.refresh();
        }
        return metrics;
    }

    @Getter
    public static final class OffloadTopicMetrics {
        private final Rate offloadRate;
        private final Rate readOffloadDataRate;
        private final LongAdder offloadErrorCount;
        private final LongAdder readOffloadErrorCount;
        private final LongAdder writeStorageErrorCount;
        private final StatsBuckets readLedgerLatencyBuckets;
        private final StatsBuckets readOffloadDataLatencyBuckets;
        private final StatsBuckets readOffloadIndexLatencyBuckets;

        public OffloadTopicMetrics() {
            this.offloadRate = new Rate();
            this.readOffloadDataRate = new Rate();
            this.offloadErrorCount = new LongAdder();
            this.readOffloadErrorCount = new LongAdder();
            this.writeStorageErrorCount = new LongAdder();
            this.readLedgerLatencyBuckets = new StatsBuckets(READ_ENTRY_LATENCY_BUCKETS_USEC);
            this.readOffloadDataLatencyBuckets = new StatsBuckets(READ_ENTRY_LATENCY_BUCKETS_USEC);
            this.readOffloadIndexLatencyBuckets = new StatsBuckets(READ_ENTRY_LATENCY_BUCKETS_USEC);
        }

        private void refresh() {
            this.offloadRate.calculateRate();
            this.readOffloadDataRate.calculateRate();
            this.readLedgerLatencyBuckets.refresh();
            this.readOffloadDataLatencyBuckets.refresh();
            this.readOffloadIndexLatencyBuckets.refresh();
        }
    }
}
