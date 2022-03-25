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
package org.apache.pulsar.broker.stats.metrics;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryMXBean;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.stats.Metrics;

abstract class AbstractMetrics {

    protected static final String METRICS_VERSION_SUFFIX = "v2";

    protected static final Pattern V2_LEDGER_NAME_PATTERN = Pattern.compile("^(([^/]+)/([^/]+)/([^/]+))/(.*)$");

    protected static final double[] ENTRY_LATENCY_BUCKETS_MS =
            new double[ManagedLedgerMBeanImpl.ENTRY_LATENCY_BUCKETS_USEC.length];

    static {
        // Convert buckets boundaries from usec to millis
        for (int i = 0; i < ManagedLedgerMBeanImpl.ENTRY_LATENCY_BUCKETS_USEC.length; i++) {
            ENTRY_LATENCY_BUCKETS_MS[i] = ManagedLedgerMBeanImpl.ENTRY_LATENCY_BUCKETS_USEC[i] / 1000.0;
        }
    }

    protected static final double[] ENTRY_SIZE_BUCKETS_BYTES =
            new double[ManagedLedgerMBeanImpl.ENTRY_SIZE_BUCKETS_BYTES.length];

    static {
        // Convert buckets boundaries from usec to millis
        for (int i = 0; i < ManagedLedgerMBeanImpl.ENTRY_SIZE_BUCKETS_BYTES.length; i++) {
            ENTRY_SIZE_BUCKETS_BYTES[i] = ManagedLedgerMBeanImpl.ENTRY_SIZE_BUCKETS_BYTES[i];
        }
    }

    // simple abstract for the buckets, their boundaries and pre-calculated keys
    // pre-calculating the keys avoids a lot of object allocations during metric collection
    static class Buckets {
        private final double[] boundaries;
        private final String[] bucketKeys;

        Buckets(String metricKey, double[] boundaries) {
            this.boundaries = boundaries;
            this.bucketKeys = generateBucketKeys(metricKey, boundaries);
        }

        private static String[] generateBucketKeys(String mkey, double[] boundaries) {
            String[] keys = new String[boundaries.length + 1];
            for (int i = 0; i < boundaries.length + 1; i++) {
                String bucketKey;
                double value;

                // example of key : "<metric_key>_0.0_0.5"
                if (i == 0 && boundaries.length > 0) {
                    bucketKey = String.format("%s_0.0_%1.1f", mkey, boundaries[i]);
                } else if (i < boundaries.length) {
                    bucketKey = String.format("%s_%1.1f_%1.1f", mkey, boundaries[i - 1], boundaries[i]);
                } else {
                    bucketKey = String.format("%s_OVERFLOW", mkey);
                }
                keys[i] = bucketKey;
            }
            return keys;
        }

        public void populateBucketEntries(Map<String, Double> map, long[] bucketValues, int period) {
            // bucket values should be one more that the boundaries to have the last element as OVERFLOW
            if (bucketValues != null && bucketValues.length != boundaries.length + 1) {
                throw new RuntimeException("Bucket boundary and value array length mismatch");
            }

            for (int i = 0; i < boundaries.length + 1; i++) {
                double value = (bucketValues == null) ? 0.0D : ((double) bucketValues[i] / (period > 0 ? period : 1));
                map.compute(bucketKeys[i], (key, currentValue) -> (currentValue == null ? 0.0d : currentValue) + value);
            }
        }
    }


    protected final PulsarService pulsar;

    abstract List<Metrics> generate();

    AbstractMetrics(PulsarService pulsar) {
        this.pulsar = pulsar;
    }

    /**
     * Creates a metrics with empty immutable dimension.
     * <p>
     * Use this for metrics that doesn't need any dimension - i.e global metrics
     *
     * @return
     */
    protected Metrics createMetrics() {
        return createMetrics(new HashMap<String, String>());
    }

    protected Metrics createMetrics(Map<String, String> dimensionMap) {
        // create with current version
        return Metrics.create(dimensionMap);
    }

    /**
     * Returns the managed ledger cache statistics from ML factory.
     *
     * @return
     */
    protected ManagedLedgerFactoryMXBean getManagedLedgerCacheStats() {
        return ((ManagedLedgerFactoryImpl) pulsar.getManagedLedgerFactory()).getCacheStats();
    }

    /**
     * Returns managed ledgers map from ML factory.
     *
     * @return
     */
    protected Map<String, ManagedLedgerImpl> getManagedLedgers() {
        return ((ManagedLedgerFactoryImpl) pulsar.getManagedLedgerFactory()).getManagedLedgers();
    }

    protected String getLocalClusterName() {
        return pulsar.getConfiguration().getClusterName();
    }

    protected double average(List<Double> values) {
        double average = 0;
        if (values.size() > 0) {
            double sum = 0;
            for (Double value : values) {
                sum += value;
            }
            average = sum / values.size();
        }
        return average;
    }

    protected double sum(List<Double> values) {
        double sum = 0;
        if (values.size() > 0) {
            for (Double value : values) {
                sum += value;
            }
        }
        return sum;
    }

    protected String parseNamespaceFromLedgerName(String ledgerName) {
        Matcher m = V2_LEDGER_NAME_PATTERN.matcher(ledgerName);

        if (m.matches()) {
            return m.group(1);
        } else {
            throw new RuntimeException("Failed to parse the namespace from ledger name : " + ledgerName);
        }
    }

    /**
     * Creates a dimension key for metrics.
     *
     * @param namespace Namespace of metric
     * @return
     */
    protected Metrics createMetricsByDimension(String namespace) {
        Map<String, String> dimensionMap = Maps.newHashMap();

        dimensionMap.put("namespace", namespace);

        return createMetrics(dimensionMap);
    }

    /**
     * Creates a dimension key for replication metrics.
     *
     * @param namespace
     * @param fromClusterName
     * @param toClusterName
     * @return
     */
    protected Metrics createMetricsByDimension(String namespace, String fromClusterName, String toClusterName) {
        Map<String, String> dimensionMap = Maps.newHashMap();

        dimensionMap.put("namespace", namespace);
        dimensionMap.put("from_cluster", fromClusterName);
        dimensionMap.put("to_cluster", toClusterName);

        return createMetrics(dimensionMap);
    }

    protected void populateAggregationMap(Map<String, List<Double>> map, String mkey, double value) {
        map.computeIfAbsent(mkey, __ -> Lists.newArrayList()).add(value);
    }

    protected void populateAggregationMapWithSum(Map<String, Double> map, String mkey, double value) {
        Double val = map.getOrDefault(mkey, 0.0);
        map.put(mkey, val + value);
    }

    protected void populateMaxMap(Map<String, Long> map, String mkey, long value) {
        Long existingValue = map.get(mkey);
        if (existingValue == null || value > existingValue) {
            map.put(mkey, value);
        }
    }

    /**
     * Helper to manage populating topics map.
     *
     * @param ledgersByDimensionMap
     * @param metrics
     * @param ledger
     */
    protected void populateDimensionMap(Map<Metrics, List<ManagedLedgerImpl>> ledgersByDimensionMap, Metrics metrics,
            ManagedLedgerImpl ledger) {
        ledgersByDimensionMap.computeIfAbsent(metrics, __ -> Lists.newArrayList()).add(ledger);
    }

    protected void populateDimensionMap(Map<Metrics, List<TopicStats>> topicsStatsByDimensionMap,
            Metrics metrics, TopicStats destStats) {
        topicsStatsByDimensionMap.computeIfAbsent(metrics, __ -> Lists.newArrayList()).add(destStats);
    }
}
