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
package org.apache.pulsar.broker.delayed.bucket;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.pulsar.common.policies.data.stats.TopicMetricBean;

public class BucketDelayedMessageIndexStats {

    private static final long[] BUCKETS = new long[]{50, 100, 500, 1000, 5000, 30000, 60000};

    enum State {
        succeed,
        failed,
        all
    }

    enum Type {
        create,
        load,
        delete,
        merge
    }

    private static final String BUCKET_TOTAL_NAME = "pulsar_delayed_message_index_bucket_total";
    private static final String INDEX_LOADED_NAME = "pulsar_delayed_message_index_loaded";
    private static final String SNAPSHOT_SIZE_BYTES_NAME = "pulsar_delayed_message_index_bucket_snapshot_size_bytes";
    private static final String OP_COUNT_NAME = "pulsar_delayed_message_index_bucket_op_count";
    private static final String OP_LATENCY_NAME = "pulsar_delayed_message_index_bucket_op_latency_ms";

    private final AtomicInteger delayedMessageIndexBucketTotal = new AtomicInteger();
    private final AtomicLong delayedMessageIndexLoaded = new AtomicLong();
    private final AtomicLong delayedMessageIndexBucketSnapshotSizeBytes = new AtomicLong();
    private final Map<String, StatsBuckets> delayedMessageIndexBucketOpLatencyMs = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> delayedMessageIndexBucketOpCount = new ConcurrentHashMap<>();

    public BucketDelayedMessageIndexStats() {
    }

    public Map<String, TopicMetricBean> genTopicMetricMap() {
        Map<String, TopicMetricBean> metrics = new HashMap<>();

        metrics.put(BUCKET_TOTAL_NAME,
                new TopicMetricBean(BUCKET_TOTAL_NAME, delayedMessageIndexBucketTotal.get(), null));

        metrics.put(INDEX_LOADED_NAME,
                new TopicMetricBean(INDEX_LOADED_NAME, delayedMessageIndexLoaded.get(), null));

        metrics.put(SNAPSHOT_SIZE_BYTES_NAME,
                new TopicMetricBean(SNAPSHOT_SIZE_BYTES_NAME, delayedMessageIndexBucketSnapshotSizeBytes.get(), null));

        delayedMessageIndexBucketOpCount.forEach((k, count) -> {
            String[] labels = splitKey(k);
            String[] labelsAndValues = new String[] {"state", labels[0], "type", labels[1]};
            String key = OP_COUNT_NAME + joinKey(labelsAndValues);
            metrics.put(key, new TopicMetricBean(OP_COUNT_NAME, count.sumThenReset(), labelsAndValues));
        });

        delayedMessageIndexBucketOpLatencyMs.forEach((typeName, statsBuckets) -> {
            statsBuckets.refresh();
            long[] buckets = statsBuckets.getBuckets();
            for (int i = 0; i < buckets.length; i++) {
                long count = buckets[i];
                if (count == 0L) {
                    continue;
                }
                String quantile;
                if (i == BUCKETS.length) {
                    quantile = "overflow";
                } else {
                    quantile = String.valueOf(BUCKETS[i]);
                }
                String[] labelsAndValues = new String[] {"type", typeName, "quantile", quantile};
                String key = OP_LATENCY_NAME + joinKey(labelsAndValues);

                metrics.put(key, new TopicMetricBean(OP_LATENCY_NAME, count, labelsAndValues));
            }
            String[] labelsAndValues = new String[] {"type", typeName};
            metrics.put(OP_LATENCY_NAME + "_count" + joinKey(labelsAndValues),
                    new TopicMetricBean(OP_LATENCY_NAME + "_count", statsBuckets.getCount(), labelsAndValues));
            metrics.put(OP_LATENCY_NAME + "_sum" + joinKey(labelsAndValues),
                    new TopicMetricBean(OP_LATENCY_NAME + "_sum", statsBuckets.getSum(), labelsAndValues));
        });

        return metrics;
    }

    public void recordNumOfBuckets(int numOfBuckets) {
        delayedMessageIndexBucketTotal.set(numOfBuckets);
    }

    public void recordDelayedMessageIndexLoaded(long num) {
        delayedMessageIndexLoaded.set(num);
    }

    public void recordBucketSnapshotSizeBytes(long sizeBytes) {
        delayedMessageIndexBucketSnapshotSizeBytes.set(sizeBytes);
    }

    public void recordTriggerEvent(Type eventType) {
        delayedMessageIndexBucketOpCount.computeIfAbsent(joinKey(State.all.name(), eventType.name()),
                k -> new LongAdder()).increment();
    }

    public void recordSuccessEvent(Type eventType, long cost) {
        delayedMessageIndexBucketOpCount.computeIfAbsent(joinKey(State.succeed.name(), eventType.name()),
                k -> new LongAdder()).increment();
        delayedMessageIndexBucketOpLatencyMs.computeIfAbsent(eventType.name(),
                k -> new StatsBuckets(BUCKETS)).addValue(cost);
    }

    public void recordFailEvent(Type eventType) {
        delayedMessageIndexBucketOpCount.computeIfAbsent(joinKey(State.failed.name(), eventType.name()),
                k -> new LongAdder()).increment();
    }

    public static String joinKey(String... values) {
        return String.join("_", values);
    }

    public static String[] splitKey(String key) {
        return key.split("_");
    }
}
