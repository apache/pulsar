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

import static org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl.ENTRY_LATENCY_BUCKETS_USEC;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.Map;
import org.apache.pulsar.common.stats.Metrics;

public class NamespaceStats {

    public double msgRateIn;
    public double msgThroughputIn;
    public double msgRateOut;
    public double msgThroughputOut;
    public double storageSize;
    public double msgBacklog;
    public double msgReplBacklog;
    public double maxMsgReplDelayInSeconds;
    public int consumerCount;
    public int producerCount;
    public int replicatorCount;
    public int subsCount;
    public static final String BRK_ADD_ENTRY_LATENCY_PREFIX = "brk_AddEntryLatencyBuckets";
    public long[] addLatencyBucket = new long[ENTRY_LATENCY_BUCKETS_USEC.length + 1];
    public static final String[] ADD_LATENCY_BUCKET_KEYS = new String[ENTRY_LATENCY_BUCKETS_USEC.length + 1];
    private int ratePeriodInSeconds = 1;

    static {
        // create static ref for add-latency-bucket keys to avoid new object allocation on every stats call.
        for (int i = 0; i < ENTRY_LATENCY_BUCKETS_USEC.length + 1; i++) {
            String key;
            // example of key : "<metric_key>_0.0_0.5"
            if (i == 0 && ENTRY_LATENCY_BUCKETS_USEC.length > 0) {
                key = String.format("%s_0.0_%1.1f",
                        BRK_ADD_ENTRY_LATENCY_PREFIX, ENTRY_LATENCY_BUCKETS_USEC[i] / 1000.0);
            } else if (i < ENTRY_LATENCY_BUCKETS_USEC.length) {
                key = String.format("%s_%1.1f_%1.1f",
                        BRK_ADD_ENTRY_LATENCY_PREFIX, ENTRY_LATENCY_BUCKETS_USEC[i - 1] / 1000.0,
                        ENTRY_LATENCY_BUCKETS_USEC[i] / 1000.0);
            } else {
                key = String.format("%s_OVERFLOW", BRK_ADD_ENTRY_LATENCY_PREFIX);
            }
            ADD_LATENCY_BUCKET_KEYS[i] = key;
        }
    }

    public NamespaceStats(int ratePeriodInSeconds) {
        this.ratePeriodInSeconds = Math.max(1, ratePeriodInSeconds);
        reset();
    }

    public void reset() {
        this.msgRateIn = 0;
        this.msgThroughputIn = 0;
        this.msgRateOut = 0;
        this.msgThroughputOut = 0;
        this.storageSize = 0;
        this.msgBacklog = 0;
        this.msgReplBacklog = 0;
        this.maxMsgReplDelayInSeconds = 0;
        this.consumerCount = 0;
        this.producerCount = 0;
        this.replicatorCount = 0;
        this.subsCount = 0;
        clear(addLatencyBucket);
    }

    public Metrics add(String namespace) {

        Map<String, String> dimensionMap = Maps.newHashMap();
        dimensionMap.put("namespace", namespace);
        Metrics dMetrics = Metrics.create(dimensionMap);
        dMetrics.put("brk_in_rate", msgRateIn);
        dMetrics.put("brk_in_tp_rate", msgThroughputIn);
        dMetrics.put("brk_out_rate", msgRateOut);
        dMetrics.put("brk_out_tp_rate", msgThroughputOut);
        dMetrics.put("brk_storage_size", storageSize);
        dMetrics.put("brk_no_of_producers", producerCount);
        dMetrics.put("brk_no_of_subscriptions", subsCount);
        dMetrics.put("brk_no_of_replicators", replicatorCount);
        dMetrics.put("brk_no_of_consumers", consumerCount);
        dMetrics.put("brk_msg_backlog", msgBacklog);
        dMetrics.put("brk_replication_backlog", msgReplBacklog);
        dMetrics.put("brk_max_replication_delay_second", maxMsgReplDelayInSeconds);
        // add add-latency metrics
        for (int i = 0; i < this.addLatencyBucket.length; i++) {
            dMetrics.put(ADD_LATENCY_BUCKET_KEYS[i], this.addLatencyBucket[i] / ratePeriodInSeconds);
        }
        return dMetrics;

    }

    public static void add(long[] src, long[] dest) {
        if (src != null && dest != null && src.length == dest.length) {
            for (int i = 0; i < src.length; i++) {
                dest[i] += src[i];
            }
        }
    }

    public static void clear(long[] list) {
        if (list != null) {
            Arrays.fill(list, 0);
        }
    }

}
