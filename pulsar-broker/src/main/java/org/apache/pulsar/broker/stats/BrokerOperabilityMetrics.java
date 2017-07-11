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
        this.topicLoadStats = new DimensionStats("topic_load_times", 60);
        this.zkWriteLatencyStats = new DimensionStats("zk_write_latency", 60);
        this.zkReadLatencyStats = new DimensionStats("zk_read_latency", 60);
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

        dMetrics.put("brk_" + dimensionName + "_time_mean_ms", stats.getMeanDimension());
        dMetrics.put("brk_" + dimensionName + "_time_median_ms", stats.getMedianDimension());
        dMetrics.put("brk_" + dimensionName + "_time_75percentile_ms", stats.getDimension75());
        dMetrics.put("brk_" + dimensionName + "_time_95percentile_ms", stats.getDimension95());
        dMetrics.put("brk_" + dimensionName + "_time_99_percentile_ms", stats.getDimension99());
        dMetrics.put("brk_" + dimensionName + "_time_99_9_percentile_ms", stats.getDimension999());
        dMetrics.put("brk_" + dimensionName + "_time_99_99_percentile_ms", stats.getDimension9999());
        dMetrics.put("brk_" + dimensionName + "_rate_s", stats.getDimensionCount());

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
