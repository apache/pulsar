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
package org.apache.pulsar.broker.stats;

import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.common.stats.Metrics;

/**
 */
public class BrokerOperabilityMetrics {
    private final List<Metrics> metricsList;
    private final String localCluster;
    private final DimensionStats topicLoadStats;
    private final DimensionStats zkWriteLatencyStats;
    private final DimensionStats zkReadLatencyStats;
    private final String brokerName;

    public BrokerOperabilityMetrics(String localCluster, String brokerName) {
        this.metricsList = new ArrayList<>();
        this.localCluster = localCluster;
        this.topicLoadStats = new DimensionStats();
        this.zkWriteLatencyStats = new DimensionStats();
        this.zkReadLatencyStats = new DimensionStats();
        this.brokerName = brokerName;
    }

    public List<Metrics> getMetrics() {
        generate();
        return metricsList;
    }

    private void generate() {
        metricsList.add(getTopicLoadMetrics());
        metricsList.add(getZkWriteLatencyMetrics());
        metricsList.add(getZkReadLatencyMetrics());
    }

    Metrics getTopicLoadMetrics() {
        return getDimensionMetrics("topic_load_times", "topic_load", topicLoadStats);
    }

    Metrics getZkWriteLatencyMetrics() {
        return getDimensionMetrics("zk_write_latency", "zk_write_latency", zkWriteLatencyStats);
    }

    Metrics getZkReadLatencyMetrics() {
        return getDimensionMetrics("zk_read_latency", "zk_read_latency", zkReadLatencyStats);
    }

    Metrics getDimensionMetrics(String metricsName, String dimensionName, DimensionStats stats) {
        Map<String, String> dimensionMap = Maps.newHashMap();
        dimensionMap.put("broker", brokerName);
        dimensionMap.put("cluster", localCluster);
        dimensionMap.put("metric", metricsName);
        Metrics dMetrics = Metrics.create(dimensionMap);

        stats.updateStats();

        dMetrics.put("brk_" + dimensionName + "_time_mean_ms", stats.meanDimensionMs);
        dMetrics.put("brk_" + dimensionName + "_time_median_ms", stats.medianDimensionMs);
        dMetrics.put("brk_" + dimensionName + "_time_95percentile_ms", stats.dimension95Ms);
        dMetrics.put("brk_" + dimensionName + "_time_99_percentile_ms", stats.dimension99Ms);
        dMetrics.put("brk_" + dimensionName + "_time_99_9_percentile_ms", stats.dimension999Ms);
        dMetrics.put("brk_" + dimensionName + "_time_99_99_percentile_ms", stats.dimension9999Ms);
        dMetrics.put("brk_" + dimensionName + "_rate_s", (1000 * stats.dimensionCounts) / stats.elapsedIntervalMs);

        return dMetrics;
    }

    public void reset() {
        metricsList.clear();
    }

    public void recordTopicLoadTimeValue(long topicLoadLatencyMs) {
        topicLoadStats.recordDimensionTimeValue(topicLoadLatencyMs, TimeUnit.MILLISECONDS);
    }

    public void recordZkWriteLatencyTimeValue(long topicLoadLatencyMs) {
        zkWriteLatencyStats.recordDimensionTimeValue(topicLoadLatencyMs, TimeUnit.MILLISECONDS);
    }

    public void recordZkReadLatencyTimeValue(long topicLoadLatencyMs) {
        zkReadLatencyStats.recordDimensionTimeValue(topicLoadLatencyMs, TimeUnit.MILLISECONDS);
    }
}
