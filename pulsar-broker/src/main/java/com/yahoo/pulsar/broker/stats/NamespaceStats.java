/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.stats;

import java.util.Map;

import com.google.common.collect.Maps;

public class NamespaceStats {

    public double msgRateIn;
    public double msgThroughputIn;
    public double msgRateOut;
    public double msgThroughputOut;
    public double storageSize;
    public double msgBacklog;
    public double msgReplBacklog;
    public int consumerCount;
    public int producerCount;
    public int replicatorCount;
    public int subsCount;

    public NamespaceStats() {
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
        this.consumerCount = 0;
        this.producerCount = 0;
        this.replicatorCount = 0;
        this.subsCount = 0;
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

        return dMetrics;

    }

}
